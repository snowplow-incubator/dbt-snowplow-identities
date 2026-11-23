"""Offline SQL regressions; these do not replace the Databricks integration run.

Render the package's Jinja, parse as Databricks SQL, and execute translated SQL
in DuckDB against the existing dbt unit fixtures. Warehouse access and dbt
materializations are deliberately outside this test's scope.
"""

from collections import Counter
from pathlib import Path
from types import SimpleNamespace
import datetime
import hashlib

import duckdb
import jinja2
import pytest
import sqlglot
import yaml


ROOT = Path(__file__).resolve().parents[2]
MAPPING = "snowplow_identities_identifier_mapping"
CASES = yaml.safe_load((ROOT / "models" / f"{MAPPING}_unit_tests.yml").read_text())["unit_tests"]


def render_context(adapter_type="databricks", incremental=False, variables=None):
    variables = variables or {}
    package = SimpleNamespace()
    namespaces = {"snowplow_identities": package}

    def dispatch(name, namespace):
        prefixes = [adapter_type]
        if adapter_type == "databricks":
            prefixes.append("spark")
        prefixes.append("default")
        for prefix in prefixes:
            candidate = getattr(namespaces[namespace], f"{prefix}__{name}", None)
            if candidate is not None:
                return candidate
        raise AssertionError(f"Missing dispatch: {adapter_type} {namespace}.{name}")

    def surrogate_key(fields):
        fields = [f"coalesce(cast({field} as string), '_dbt_utils_surrogate_key_null_')" for field in fields]
        return "md5(concat(" + ", '-', ".join(fields) + "))"

    utilities = SimpleNamespace(
        set_query_tag=lambda *args: "",
        get_value_by_target_type=lambda **kwargs: kwargs.get(f"{adapter_type}_val"),
        is_run_with_new_events=lambda *args: "true",
        app_id_filter=lambda *args: "true",
        current_timestamp_in_utc=lambda: "current_timestamp()",
    )
    namespaces["snowplow_utils"] = utilities
    context = dict(
        target=SimpleNamespace(type=adapter_type),
        adapter=SimpleNamespace(dispatch=dispatch),
        config=lambda **kwargs: "",
        var=lambda key, default=None: variables.get(key, default),
        ref=lambda name: name,
        source=lambda schema, name: f"{schema}_{name}",
        this="historical",
        is_incremental=lambda: incremental,
        snowplow_identities=package,
        snowplow_utils=utilities,
        dbt_utils=SimpleNamespace(generate_surrogate_key=surrogate_key),
        **{"return": lambda value: value},
    )
    env = jinja2.Environment(undefined=jinja2.StrictUndefined, extensions=["jinja2.ext.do"])
    paths = list((ROOT / "macros").rglob("*.sql"))
    for path in paths:
        module = env.from_string(path.read_text()).make_module(context)
        for name in dir(module):
            if isinstance(getattr(module, name), jinja2.runtime.Macro):
                setattr(package, name, getattr(module, name))
                context[name] = getattr(module, name)
    # Native dbt unit tests exercise watermark windows; this harness checks SQL
    # expressions and output schemas without warehouse-backed relation metadata.
    package.get_incremental_filter = lambda *args, **kwargs: "true"
    return env, context


def render_model(name, adapter_type="databricks", incremental=False, variables=None):
    env, context = render_context(adapter_type, incremental, variables)
    path, = (ROOT / "models").rglob(f"{name}.sql")
    return env.from_string(path.read_text()).render(context)


def duck_sql(sql, source="databricks"):
    return sqlglot.transpile(sql, read=source, write="duckdb")[0]


def is_timestamp(column):
    return column.endswith(("_at", "_tstamp"))


def fixture_table(connection, fixture):
    name = "historical" if fixture["input"] == "this" else fixture["input"].split("'")[1]
    if fixture.get("format") == "sql":
        connection.execute(f"create table {name} as " + duck_sql(fixture["rows"], "snowflake"))
        return
    rows = fixture["rows"]
    # Empty dbt fixtures get their schema from the warehouse. Supply those
    # dependency columns locally without manufacturing any rows.
    columns = {key for row in rows for key in row}
    columns |= {"snowplow_id", "active_snowplow_id", "created_at", "merged_at", "model_tstamp"}
    columns |= {"uuid", "id_type", "id_value", "first_app_id", "last_app_id", "first_seen_at", "last_seen_at", "first_seen_event_id"}
    columns = sorted(columns)
    definition = ", ".join(f"{col} {'timestamp' if is_timestamp(col) else 'varchar'}" for col in columns)
    connection.execute(f"create table {name} ({definition})")
    if rows:
        connection.executemany(
            f"insert into {name} values ({', '.join('?' for _ in columns)})",
            [[row.get(col) for col in columns] for row in rows],
        )


def normalized(value, column):
    if value is not None and is_timestamp(column):
        return str(datetime.datetime.fromisoformat(str(value)))
    return value


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_existing_identifier_mapping_scenarios(case):
    variables = case.get("overrides", {}).get("vars", {})
    sql = render_model(MAPPING, incremental=True, variables=variables)
    with duckdb.connect() as connection:
        for fixture in case["given"]:
            fixture_table(connection, fixture)
        connection.execute("create table result as " + duck_sql(sql))
        expected = case["expect"]["rows"]
        columns = list(expected[0])
        actual = connection.execute("select " + ", ".join(columns) + " from result").fetchall()
        assert Counter(tuple(normalized(v, col) for col, v in zip(columns, row)) for row in actual) == Counter(
            tuple(normalized(row[col], col) for col in columns) for row in expected
        )


@pytest.mark.parametrize("adapter_type", ["databricks", "snowflake", "bigquery"])
def test_identifier_hashes_preserve_normalization(adapter_type):
    _, context = render_context(adapter_type)
    expression = context["snowplow_identities"].hash_id_value("value")
    sql = f"select {expression} from (select '  Alice@Example.COM  ' as value)"
    with duckdb.connect() as connection:
        assert connection.execute(duck_sql(sql, adapter_type)).fetchone()[0].lower() == hashlib.sha256(b"alice@example.com").hexdigest()


def test_identity_extraction_handles_empty_and_null_contexts():
    with duckdb.connect() as connection:
        connection.execute("set timezone = 'UTC'")
        # SQLGlot leaves Databricks GET unchanged; model its documented
        # zero-based, null-on-out-of-bounds behaviour in DuckDB.
        connection.execute("create macro get(a, i) as case when i < 0 then null else list_extract(a, i + 1) end")
        connection.execute("""
            create table snowplow_identities_base_events_this_run (
                contexts_com_snowplowanalytics_snowplow_identity_2 struct(snowplow_id varchar, created_at timestamp)[],
                event_id varchar, app_id varchar, domain_userid varchar, user_id varchar,
                derived_tstamp timestamp, collector_tstamp timestamp
            )
        """)
        timestamp = datetime.datetime(2026, 1, 1)
        contexts = [[{"snowplow_id": "sp_first", "created_at": timestamp}, {"snowplow_id": "sp_second", "created_at": timestamp}], [], None]
        connection.executemany(
            "insert into snowplow_identities_base_events_this_run values (?, ?, 'web', 'visitor', null, ?, ?)",
            [(context, f"evt_{i}", timestamp, timestamp) for i, context in enumerate(contexts)],
        )
        _, context = render_context()
        fields = str(context["snowplow_identities"].get_identity_fields()).strip().rstrip(",")
        rows = connection.execute(duck_sql(
            "select event_id, " + fields + " from snowplow_identities_base_events_this_run order by event_id"
        )).fetchall()
        assert rows == [("evt_0", "sp_first", timestamp.replace(tzinfo=datetime.timezone.utc)), ("evt_1", None, None), ("evt_2", None, None)]


def test_merge_extraction_preserves_children_and_deduplicates_events():
    _, context = render_context()
    timestamp = datetime.datetime(2026, 1, 1)
    with duckdb.connect() as connection:
        connection.execute("""
            create table snowplow_identities_merge_events_this_run (
                active_snowplow_id varchar, merge_event_id varchar, load_tstamp timestamp,
                merged struct(snowplow_id varchar, created_at timestamp, merged_at timestamp, triggering_event_id varchar)[]
            )
        """)
        children = [{"snowplow_id": child, "created_at": timestamp, "merged_at": timestamp, "triggering_event_id": event} for child, event in [("sp_child_a", "evt_b"), ("sp_child_a", "evt_a"), ("sp_child_b", "evt_c")]]
        connection.executemany("insert into snowplow_identities_merge_events_this_run values (?, ?, ?, ?)", [(parent, "merge_a", timestamp, values) for parent, values in [("sp_parent", children), ("sp_empty", []), ("sp_null", None)]])
        sql = context["snowplow_identities"].extract_merged("snowplow_identities_merge_events_this_run")
        rows = connection.execute(duck_sql(sql)).fetchall()
        assert set(rows) == {("sp_parent", "sp_child_a", timestamp, "merge_a", "evt_a", timestamp), ("sp_parent", "sp_child_b", timestamp, "merge_a", "evt_c", timestamp)}


@pytest.mark.parametrize("model", [
    "snowplow_identities_stg_identity_events",
    "snowplow_identities_merge_events",
    "snowplow_identities_identities",
    "snowplow_identities_identifier_mapping_base",
    "snowplow_identities_snowplow_id_mapping",
])
@pytest.mark.parametrize("incremental", [False, True])
def test_all_incremental_models_produce_their_partition_column(model, incremental):
    parsed = sqlglot.parse_one(render_model(model, incremental=incremental), read="databricks")
    assert "load_tstamp_date" in parsed.named_selects
