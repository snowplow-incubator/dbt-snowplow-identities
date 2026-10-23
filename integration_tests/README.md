# snowplow-identities integration tests

Run from this directory:

```bash
bash .scripts/integration_test.sh -d {warehouse}
```

Supported targets are `snowflake`, `bigquery`, `databricks`, or `all`.
The suite seeds recorded identity-service events, runs one full refresh and three
incremental batches, and compares the four public outputs with reviewed expected
CSVs. It repeats those runs with `snowplow__merge_limit_collapse: true`. In 0.2.0
that option keeps every owner and sets `is_preferred`; it does not delete rows.
The expected seed records both flag settings, including the TTL reappearance
case. Unique-identifier scenarios are excluded from the opt-in comparison because
the heuristic is only safe when the service has no configured unique identifiers.
All dbt unit tests run on all three warehouses, including typed Databricks merge
payloads. See [data/README.md](data/README.md) for fixture provenance.

Databricks uses `DATABRICKS_TEST_HOST`, `DATABRICKS_TEST_HTTP_PATH` and
`DATABRICKS_TEST_TOKEN`. Set `DATABRICKS_TEST_CATALOG` for a Unity Catalog test
catalog; the default is `hive_metastore`. The test identity needs permission to
create and drop isolated `gh_sp_identities_dbt_*` test schemas. CI uses Python 3.12
and the latest dbt Core and adapter 1.x releases, with separate schemas per workflow
run. Snowflake CI uses `SNOWFLAKE_TEST_PRIVATE_KEY`.

## Offline SQL regressions

```bash
pip install -r python/requirements.txt
python -m pytest python -q
```

These render package SQL, parse it as Databricks SQL and execute translated queries
in DuckDB against the existing mapping fixtures. They also check partition-column
projection, normalized hashing, and typed empty/null identity and merge payloads.
They do not validate warehouse materializations, permissions, runtime planning or
watermark advancement; those require the native dbt integration and unit tests.

## Upgrade validation

The `pr_tests` manual workflow accepts `test_upgrade: true`. After the normal suite,
it builds the published 0.1.0 package in another isolated schema, applies the
full refresh documented in MIGRATION.md to the populated 0.1.0 tables.
It checks table-to-view conversion and compares the upgraded outputs with the same
expected fixtures, including another incremental run for idempotency. Workflow
cleanup removes both sets of test schemas.
