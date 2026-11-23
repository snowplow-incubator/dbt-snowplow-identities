"""Exercise the startup guard without warehouse mutations or credentials."""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from test_incremental_state import real_context


MODELS = ['snowplow_identities_merge_events', 'snowplow_identities_snowplow_id_mapping',
          'snowplow_identities_identifier_mapping']
IDS = [f'model.snowplow_identities.{name}' for name in MODELS]


def guard(selected, columns, full_refresh=False, which='run', refresh_disabled=False):
    relations = {name: SimpleNamespace(database='test', schema='derived', identifier=name) for name in MODELS}
    adapter = SimpleNamespace(
        get_relation=Mock(side_effect=lambda **kwargs: relations[kwargs['identifier']]
                         if kwargs['identifier'] in columns else None),
        get_columns_in_relation=lambda relation: [SimpleNamespace(name=name) for name in columns[relation.identifier]],
    )
    nodes = {uid: {
        'unique_id': uid, 'resource_type': 'model', 'package_name': 'snowplow_identities',
        'config': {'materialized': 'incremental', 'full_refresh': False if refresh_disabled else None},
    } for uid in IDS}
    _, context = real_context(False, {},
        ref=lambda name: relations[name], adapter=adapter,
        flags=SimpleNamespace(WHICH=which, FULL_REFRESH=full_refresh),
        selected_resources=selected, graph=SimpleNamespace(nodes=nodes))
    return context['snowplow_identities'].check_upgrade, adapter.get_relation


@pytest.mark.parametrize('selected', [[], ['model.consumer.unrelated'], ['test.snowplow_identities.some_test']])
def test_unrelated_selection_does_not_inspect_legacy_package(selected):
    check, get_relation = guard(selected, {MODELS[0]: ['derived_tstamp']})
    check()
    get_relation.assert_not_called()


@pytest.mark.parametrize('which', ['test', 'compile', 'run-operation'])
def test_read_only_commands_do_not_require_an_upgrade(which):
    check, get_relation = guard(IDS, {MODELS[0]: ['derived_tstamp']}, which=which)
    check()
    get_relation.assert_not_called()


@pytest.mark.parametrize('columns', [
    {MODELS[0]: ['derived_tstamp']},
    {MODELS[1]: ['merged_at']},
    {MODELS[0]: ['load_tstamp'], MODELS[1]: ['merged_at']},
])
def test_either_legacy_table_requires_coordinated_upgrade(columns):
    check, _ = guard([IDS[0]], columns)
    with pytest.raises(ValueError, match='dbt run --full-refresh --select package:snowplow_identities'):
        check()


@pytest.mark.parametrize('which', ['run', 'build'])
def test_partial_full_refresh_does_not_bypass_legacy_guard(which):
    check, _ = guard([IDS[0]], {MODELS[1]: ['merged_at']}, full_refresh=True, which=which)
    with pytest.raises(ValueError, match='Include every enabled package model'):
        check()


def test_configured_full_refresh_override_cannot_bypass_legacy_guard():
    check, _ = guard(IDS, {MODELS[1]: ['merged_at']}, full_refresh=True, refresh_disabled=True)
    with pytest.raises(ValueError, match='full_refresh: false'):
        check()


def test_full_package_refresh_allows_the_upgrade():
    check, get_relation = guard(IDS + ['model.consumer.unrelated'],
                               {MODELS[0]: ['derived_tstamp'], MODELS[1]: ['merged_at']}, full_refresh=True)
    check()
    assert get_relation.call_count == 2


@pytest.mark.parametrize('columns', [{}, {MODELS[0]: ['LOAD_TSTAMP'], MODELS[1]: ['load_tstamp']}])
def test_new_or_upgraded_installations_allow_partial_runs(columns):
    check, _ = guard([IDS[0]], columns)
    check()
