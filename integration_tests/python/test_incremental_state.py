"""Execute the native regression fixtures locally with the real window predicates.

Native CI also runs these dbt unit tests on all three supported warehouses.
"""
from collections import Counter
from types import SimpleNamespace
import datetime
import jinja2
import duckdb
import pytest
import yaml
from test_databricks_sql import ROOT, render_context, duck_sql, fixture_table, is_timestamp


def normalized(value, column):
    if value is not None and is_timestamp(column):
        return datetime.datetime.fromisoformat(str(value)).replace(tzinfo=datetime.timezone.utc)
    return value


class MacroReturn(Exception):
    pass


def macro_return(value):
    raise MacroReturn(value)


def wrapped(macro):
    def call(*args, **kwargs):
        try:
            return macro(*args, **kwargs)
        except MacroReturn as result:
            return result.args[0]
    return call


def real_context(incremental, variables):
    env, context = render_context(incremental=incremental, variables=variables)
    context.update({
        'return': macro_return,
        'execute': True,
        'flags': SimpleNamespace(WHICH='test'),
        'dbt': SimpleNamespace(dateadd=lambda unit, count, expr: f'timestampadd({unit}, {count}, {expr})'),
        'exceptions': SimpleNamespace(raise_compiler_error=lambda message: (_ for _ in ()).throw(ValueError(message))),
    })
    context['snowplow_utils'].app_id_filter = lambda apps: 'true' if not apps else "app_id in (" + ','.join("'" + a.replace("'", "''") + "'" for a in apps) + ')'
    for path in (ROOT / 'macros').rglob('*.sql'):
        module = env.from_string(path.read_text()).make_module(context)
        for name in dir(module):
            macro = getattr(module, name)
            if isinstance(macro, jinja2.runtime.Macro):
                setattr(context['snowplow_identities'], name, wrapped(macro))
    return env, context


CASES = []
for path in ['models/snowplow_identities_snowplow_id_mapping_unit_tests.yml',
             'models/staging/snowplow_identities_stg_identity_events_unit_tests.yml',
             'models/snowplow_identities_merge_events_unit_tests.yml']:
    CASES += yaml.safe_load((ROOT / path).read_text())['unit_tests']


@pytest.mark.parametrize('case', CASES, ids=lambda case: case['name'])
def test_incremental_state(case):
    env, context = real_context(case['overrides']['macros']['is_incremental'], case['overrides']['vars'])
    path, = (ROOT / 'models').rglob(case['model'] + '.sql')
    query = env.from_string(path.read_text()).render(context)
    with duckdb.connect() as connection:
        connection.execute("set timezone = 'UTC'")
        connection.execute('create macro get(a, i) as case when i < 0 then null else list_extract(a, i + 1) end')
        for fixture in case['given']:
            if fixture.get('format') == 'sql':
                name = 'atomic_events' if fixture['input'].startswith('source(') else fixture['input'].split("'")[1]
                sql = env.from_string(fixture['rows']).render({**context, 'target': SimpleNamespace(type='bigquery')})
                connection.execute(f'create table {name} as ' + duck_sql(sql, 'bigquery'))
            else:
                if not fixture['rows']:
                    name = 'historical' if fixture['input'] == 'this' else fixture['input'].split("'")[1]
                    connection.execute(f'create table {name} (load_tstamp timestamp)')
                else:
                    fixture_table(connection, fixture)
        connection.execute('create table result as ' + duck_sql(query))
        expected = case['expect']['rows']
        if not expected:
            assert connection.execute('select count(*) from result').fetchone()[0] == 0
            return
        columns = list(expected[0])
        actual = connection.execute('select ' + ', '.join(columns) + ' from result').fetchall()
        assert Counter(tuple(normalized(v, c) for c, v in zip(columns, row)) for row in actual) == Counter(
            tuple(normalized(row[c], c) for c in columns) for row in expected)
