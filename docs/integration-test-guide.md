# Integration Test Guide

How to write, run, and maintain integration tests for dbt-snowplow-identities. Follows the Snowplow dbt package integration test pattern (used by dbt-snowplow-web, dbt-snowplow-unified, dbt-snowplow-mobile), adapted for our grouped-scenario approach.

---

## Architecture overview

```
integration_tests/
  .scripts/
    integration_test.sh              ← orchestrates seed + multi-run + test
  ci/
    profiles.yml                     ← warehouse connection profiles (env vars)
  dbt_project.yml                    ← vars, seed column types, model enablement
  packages.yml                       ← points to parent package via local: ../
  data/
    source/
      <group>_events.csv             ← seed: fake atomic.events rows, one per group
    expected/
      <group>_<table>_expected.csv   ← expected output per derived table per group
      snowflake/                     ← adapter-specific expected (only if output differs)
      bigquery/
  models/
    source/
      snowflake/
        <group>_events_stg.sql       ← parse_json on context columns
      bigquery/
        <group>_events_stg.sql       ← JSON_EXTRACT_ARRAY + STRUCT reconstruction
      default/
        <group>_events_stg.sql       ← passthrough (if Postgres ever needed)
    expected/
      <group>_<table>_expected_stg.sql  ← select from expected seed, cast as needed
    actual/
      <group>_<table>_actual.sql     ← select from real model output, exclude model_tstamp
      actual_vs_expected.yml         ← dbt_utils.equality + equal_rowcount tests
  macros/
    equality.sql                     ← custom equality macro if precision needed
```

---

## The five layers

### Layer 1: Source seed CSV

One CSV per test group, mimicking `atomic.events`. Stored in `data/source/`.

**Critical rule:** Complex types (identity contexts, merge event payloads) are stored as **JSON strings** in the CSV. The adapter-specific staging model (Layer 2) parses them into native types.

**JSON field naming convention:** Use **camelCase** in the CSV JSON (matching the Snowplow VARIANT/JSON format as it comes from the enricher). The staging models handle the mapping to each adapter's expected format.

Example CSV row for a page_view with identity context:

```csv
event_id,event_name,app_id,collector_tstamp,derived_tstamp,load_tstamp,domain_userid,user_id,contexts_com_snowplowanalytics_snowplow_identity_1,unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1,...
evt-1,page_view,web,2026-03-10 08:00:00,2026-03-10 08:00:00,2026-03-10 08:01:00,abc123,,"[{""snowplowId"":""sp_A"",""createdAt"":""2026-03-10T08:00:00""}]",,
```

Example CSV row for a merge event:

```csv
evt-7,identity_merge,web,2026-03-10 09:00:00,2026-03-10 09:00:00,2026-03-10 09:01:00,,,,"{""snowplowId"":""sp_A"",""merged"":[{""snowplowId"":""sp_B"",""mergedAt"":""2026-03-10T09:00:00"",""triggeringEventId"":""evt-7""}],""merges"":[""sp_B""]}",...
```

**Key points:**
- Double-quote JSON strings in CSV using `""` escaping (standard CSV)
- Identity context is an **array** of objects: `[{"snowplowId": ..., "createdAt": ...}]`
- Merge event payload is a **single object** (not array): `{"snowplowId": ..., "merged": [...], "merges": [...]}`
- Use `load_tstamp` to control which batch each event falls into. The Snowplow incremental manifest uses `load_tstamp` to slice data into runs
- Events without identity context: leave `contexts_com_snowplowanalytics_snowplow_identity_1` empty/NULL
- Events without merge payload: leave `unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1` empty/NULL

**Column types in dbt_project.yml:**

Declare types for every column that isn't a plain string or that might cause implicit casting issues. The seed column types section uses Jinja conditionals for cross-db compatibility:

```yaml
seeds:
  snowplow_identities_integration_tests:
    +schema: "snplw_identities_int_tests"
    source:
      basic_events:
        +column_types:
          app_id: "{{ 'string' if target.type in ['bigquery'] else 'varchar' }}"
          collector_tstamp: timestamp
          derived_tstamp: timestamp
          load_tstamp: timestamp
          # Context columns stored as JSON strings — parsed by staging models
          contexts_com_snowplowanalytics_snowplow_identity_1:
            "{{ 'string' if target.type in ['bigquery'] else 'varchar(65535)' }}"
          unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1:
            "{{ 'string' if target.type in ['bigquery'] else 'varchar(65535)' }}"
```

Use `varchar(65535)` for JSON columns on Snowflake/Postgres — default `varchar` may truncate large JSON.

### Layer 2: Adapter-specific source staging models

One staging model per adapter per test group. This is the **only** adapter-specific code. Stored in `models/source/<adapter>/`.

**Snowflake staging** — uses `parse_json()` to create VARIANT:

```sql
-- models/source/snowflake/basic_events_stg.sql
select
    * exclude (
        contexts_com_snowplowanalytics_snowplow_identity_1,
        unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1
    ),
    parse_json(contexts_com_snowplowanalytics_snowplow_identity_1)
        as contexts_com_snowplowanalytics_snowplow_identity_1,
    parse_json(unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1)
        as unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1
from {{ ref('basic_events') }}
```

That's it. Snowflake VARIANT natively supports the camelCase JSON fields. The production macros (`get_identity_fields`, `get_merge_fields`, `extract_merged`) access fields like `[0]:snowplowId::varchar` — this works directly on parsed VARIANT.

**BigQuery staging** — uses `JSON_EXTRACT_ARRAY` + struct reconstruction:

```sql
-- models/source/bigquery/basic_events_stg.sql
with prep as (
    select
        * except (
            contexts_com_snowplowanalytics_snowplow_identity_1,
            unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1
        ),
        JSON_EXTRACT_ARRAY(
            contexts_com_snowplowanalytics_snowplow_identity_1
        ) as _identity_json,
        JSON_EXTRACT(
            unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1, '$'
        ) as _merge_json
    from {{ ref('basic_events') }}
)

select
    * except (_identity_json, _merge_json),

    -- Reconstruct identity context as ARRAY<STRUCT<snowplow_id, created_at>>
    array(
        select as struct
            JSON_EXTRACT_scalar(j, '$.snowplowId') as snowplow_id,
            cast(JSON_EXTRACT_scalar(j, '$.createdAt') as timestamp) as created_at
        from unnest(_identity_json) as j
    ) as contexts_com_snowplowanalytics_snowplow_identity_1,

    -- Reconstruct merge event as struct with nested array
    struct(
        JSON_EXTRACT_scalar(_merge_json, '$.snowplowId') as snowplow_id,
        array(
            select as struct
                JSON_EXTRACT_scalar(m, '$.snowplowId') as snowplow_id,
                cast(JSON_EXTRACT_scalar(m, '$.mergedAt') as timestamp) as merged_at,
                JSON_EXTRACT_scalar(m, '$.triggeringEventId') as triggering_event_id
            from unnest(JSON_EXTRACT_ARRAY(_merge_json, '$.merged')) as m
        ) as merged,
        JSON_EXTRACT_STRING_ARRAY(_merge_json, '$.merges') as merges
    ) as unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1

from prep
```

This reconstructs the column types that the production BigQuery macros expect: the identity context as `ARRAY<STRUCT<snowplow_id STRING, created_at TIMESTAMP>>` and the merge payload with its nested `merged` array.

**Conditional enablement in dbt_project.yml:**

```yaml
models:
  snowplow_identities_integration_tests:
    +schema: "snplw_identities_int_tests"
    source:
      snowflake:
        +enabled: "{{ target.type == 'snowflake' | as_bool() }}"
      bigquery:
        +enabled: "{{ target.type == 'bigquery' | as_bool() }}"
      default:
        +enabled: "{{ target.type in ['redshift', 'postgres'] | as_bool() }}"
```

Only the staging model matching the current adapter compiles and runs.

### Layer 3: Var override — point the package at staging models

The parent package reads events from `source('atomic', 'events')` by default. Override this to point at the staging model:

```yaml
# dbt_project.yml
vars:
  snowplow_identities:
    snowplow__events: "{{ ref('basic_events_stg') }}"
    snowplow__atomic_schema: "{{ target.schema ~ '_snplw_identities_int_tests' }}"
    snowplow__start_date: '2026-03-01'
    snowplow__backfill_limit_days: 30
    snowplow__license_accepted: true
```

This makes the parent package's `base_events_this_run` read from our seeded+staged data instead of a real `atomic.events` table.

### Layer 4: Actual and expected wrapper models

**Actual models** — thin SELECTs from the real derived tables. Exclude `model_tstamp` (non-deterministic) and any other columns that vary between runs. Stored in `models/actual/`.

```sql
-- models/actual/basic_id_changes_actual.sql
select
    id_change_key,
    snowplow_id,
    previous_snowplow_id,
    effective_at,
    change_type,
    first_seen_event_id,
    first_seen_app_id
from {{ ref('snowplow_identities_id_changes') }}
```

Note: exclude `changed_at` too — it's `current_timestamp()` at model run time, not deterministic.

**Expected staging models** — select from the expected seed CSV, applying any type casts needed. Stored in `models/expected/`.

```sql
-- models/expected/basic_id_changes_expected_stg.sql
select
    id_change_key,
    snowplow_id,
    previous_snowplow_id,
    effective_at,
    change_type,
    first_seen_event_id,
    first_seen_app_id
from {{ ref('basic_id_changes_expected') }}
```

For most tables this is a simple passthrough. If a column needs adapter-specific casting (e.g., boolean representation), add a Jinja conditional.

**When expected output differs by adapter:** create adapter-specific expected CSVs in `data/expected/snowflake/` and `data/expected/bigquery/`. Use conditional enablement:

```yaml
seeds:
  expected:
    snowflake:
      +enabled: "{{ target.type == 'snowflake' | as_bool() }}"
    bigquery:
      +enabled: "{{ target.type == 'bigquery' | as_bool() }}"
```

For the identities package, expected output should be identical across adapters for most tables. The main exception is `identifier_mapping.id_type` if UNPIVOT case normalization hasn't been applied (Snowflake: `DOMAIN_USERID`, BigQuery: `domain_userid`).

### Layer 5: Comparison tests

Two tests per model in `models/actual/actual_vs_expected.yml`:

```yaml
version: 2

models:
  - name: basic_id_changes_actual
    tests:
      - dbt_utils.equal_rowcount:
          compare_model: ref('basic_id_changes_expected_stg')
      - dbt_utils.equality:
          compare_model: ref('basic_id_changes_expected_stg')

  - name: basic_new_identities_actual
    tests:
      - dbt_utils.equal_rowcount:
          compare_model: ref('basic_new_identities_expected_stg')
      - dbt_utils.equality:
          compare_model: ref('basic_new_identities_expected_stg')

  # ... repeat for all 5 derived tables per group
```

`equal_rowcount` catches missing/extra rows. `equality` catches wrong values. Together they give a clear signal: if rowcount passes but equality fails, you have wrong data. If rowcount fails, you have missing or extra rows.

---

## How to control which events appear in which run

The Snowplow incremental framework uses `load_tstamp` to determine which events are "new" in each run. Events with `load_tstamp` after the last-processed timestamp are picked up.

Assign `load_tstamp` values to create batches:

| Batch (run) | load_tstamp range | Purpose |
|---|---|---|
| 1 (full-refresh) | 2026-03-10 08:00 — 2026-03-10 09:59 | Initial data |
| 2 (incremental) | 2026-03-11 10:00 — 2026-03-11 10:59 | New events, merges |
| 3 (incremental) | 2026-03-12 10:00 — 2026-03-12 10:59 | More merges, edge cases |
| 4 (incremental) | No events | Empty run |

Use day-level separation between batches. This ensures the incremental manifest clearly separates runs. Set `snowplow__backfill_limit_days: 30` to cover the full date range.

---

## The integration test script

The script orchestrates: seed, full-refresh run, incremental runs, test.

```bash
#!/bin/bash
# .scripts/integration_test.sh

while getopts 'd:' opt; do
  case $opt in
    d) DATABASE=$OPTARG ;;
  esac
done

declare -a SUPPORTED_DATABASES=("bigquery" "snowflake")
DATABASE="$(echo $DATABASE | tr '[:upper:]' '[:lower:]')"

if [[ $DATABASE == "all" ]]; then
  DATABASES=("${SUPPORTED_DATABASES[@]}")
else
  DATABASES=($DATABASE)
fi

for db in ${DATABASES[@]}; do

  echo "Integration tests: Seeding data"
  eval "dbt seed --full-refresh --target $db" || exit 1

  echo "Integration tests: Run 1/4 (full-refresh)"
  eval "dbt run --full-refresh --vars '{snowplow__allow_refresh: true, snowplow__backfill_limit_days: 30}' --target $db" || exit 1

  for i in {2..4}; do
    echo "Integration tests: Run $i/4 (incremental)"
    eval "dbt run --target $db" || exit 1
  done

  echo "Integration tests: Testing"
  eval "dbt test --store-failures --target $db" || exit 1

  echo "Integration tests: All tests passed on $db"

done
```

Run it:

```bash
cd integration_tests
bash .scripts/integration_test.sh -d snowflake
bash .scripts/integration_test.sh -d bigquery
bash .scripts/integration_test.sh -d all
```

---

## How to add a new test scenario

### Step 1: Choose a group

- **basic** — happy-path scenarios (creation, simple merges, dedup, incremental)
- **complex_merges** — transitive chains, diamonds, multi-child merges
- **edge_cases** — late duplicates, merge-only batches, shared identifiers

If the scenario doesn't fit any group or would interfere with existing scenarios (e.g., its merges contaminate other identities' expected output), create a new group.

### Step 2: Add events to the source CSV

Add rows to `data/source/<group>_events.csv`. Use unique `event_id` values (within the group) and assign `load_tstamp` to control which run picks them up.

Checklist:
- [ ] `event_id` is unique within the CSV (unless testing dedup)
- [ ] `event_name` is `page_view` or `identity_merge`
- [ ] `load_tstamp` places the event in the correct batch
- [ ] Identity context JSON uses camelCase field names
- [ ] Merge event JSON has the correct structure
- [ ] All mandatory `atomic.events` columns are present (even if NULL)

### Step 3: Update expected output CSVs

Recalculate the expected state of **all 5 derived tables** after all 4 runs. Update:
- `data/expected/<group>_new_identities_expected.csv`
- `data/expected/<group>_snowplow_id_mapping_expected.csv`
- `data/expected/<group>_id_changes_expected.csv`
- `data/expected/<group>_id_mapping_scd_expected.csv`
- `data/expected/<group>_identifier_mapping_expected.csv`

The expected output is the **final state** after all runs complete — not intermediate states.

### Step 4: Run and verify

```bash
cd integration_tests
dbt seed --full-refresh --target snowflake
dbt run --full-refresh --vars '{snowplow__allow_refresh: true, snowplow__backfill_limit_days: 30}' --target snowflake
dbt run --target snowflake   # run 2
dbt run --target snowflake   # run 3
dbt run --target snowflake   # run 4
dbt test --store-failures --target snowflake
```

If a test fails, inspect the stored failure in `target/run_results.json` or query the failure table directly in the warehouse.

### Step 5: Debug failing tests

Common failure patterns:

| Symptom | Likely cause |
|---|---|
| `equal_rowcount` fails (actual has more rows) | An event is producing unexpected output — check dedup logic or whether an event is being processed in multiple batches |
| `equal_rowcount` fails (actual has fewer rows) | An event's identity context is NULL or malformed in the CSV — the model filters it out |
| `equality` fails on `id_type` values | UNPIVOT case difference (Snowflake uppercase vs BigQuery lowercase) — need adapter-specific expected or LOWER() normalization |
| `equality` fails on timestamps | CSV timestamp format doesn't match warehouse parsing — use ISO format `2026-03-10 08:00:00` |
| `equality` fails on `id_change_key` | Surrogate key inputs changed — regenerate expected keys or use a comparison that excludes surrogate keys |
| Model fails to compile | Staging model doesn't produce the expected column types — check `parse_json` output |

---

## How to add a new test group

### Step 1: Create seed files

```
data/source/<group>_events.csv
data/expected/<group>_new_identities_expected.csv
data/expected/<group>_snowplow_id_mapping_expected.csv
data/expected/<group>_id_changes_expected.csv
data/expected/<group>_id_mapping_scd_expected.csv
data/expected/<group>_identifier_mapping_expected.csv
```

### Step 2: Create staging models

```
models/source/snowflake/<group>_events_stg.sql
models/source/bigquery/<group>_events_stg.sql
```

Copy from an existing group's staging model and change the `ref()` to point at the new seed.

### Step 3: Create actual + expected wrapper models

For each derived table:

```
models/actual/<group>_<table>_actual.sql
models/expected/<group>_<table>_expected_stg.sql
```

### Step 4: Add comparison tests to actual_vs_expected.yml

Add `equal_rowcount` + `equality` entries for each new actual/expected pair.

### Step 5: Update dbt_project.yml

Add seed column type declarations for the new source CSV. Add conditional enablement if needed.

### Step 6: Update the var override

If using a different staging model name, update `snowplow__events` to point at it. If all groups use the same var, you may need a macro or script-level var override to switch between groups.

---

## Handling multiple groups with a single pipeline

The Snowplow incremental framework uses a single manifest and a single set of derived tables. You can't run two groups simultaneously against the same schema — they'd contaminate each other.

**Option A: Run groups sequentially with full-refresh between them.** Each group's test script does `dbt run --full-refresh` at the start, wiping previous state. This is simplest.

**Option B: Use different schemas per group.** Override `snowplow__atomic_schema` and model schema per group. More complex but allows parallel CI.

**Recommended:** Option A. Keep it simple. The test script runs each group sequentially:

```bash
for group in basic complex_merges edge_cases; do
  echo "Running group: $group"
  # Override snowplow__events to point at ${group}_events_stg
  eval "dbt seed --full-refresh --target $db" || exit 1
  eval "dbt run --full-refresh --vars '{snowplow__allow_refresh: true, snowplow__backfill_limit_days: 30, snowplow__events: ref(\"${group}_events_stg\")}' --target $db" || exit 1
  for i in {2..4}; do
    eval "dbt run --vars '{snowplow__events: ref(\"${group}_events_stg\")}' --target $db" || exit 1
  done
  eval "dbt test --select tag:${group} --store-failures --target $db" || exit 1
done
```

Tag each group's actual models and tests with the group name so `--select tag:<group>` runs only that group's comparisons.

---

## Columns to include in actual models (and which to exclude)

**Always exclude from actual models:**
- `model_tstamp` — set at run time, non-deterministic
- `changed_at` in `id_changes` — `current_timestamp()`, non-deterministic

**Always include:**
- All business-logic columns (snowplow_id, active_snowplow_id, timestamps, identifiers)
- Surrogate keys (`id_change_key`, `uuid`) — these verify that the key generation logic is correct

**Include with caution:**
- `first_seen_event_id` — deterministic but depends on dedup ordering
- `first_app_id` / `last_app_id` — deterministic

---

## Surrogate keys in expected output

The `id_change_key` and `uuid` columns are generated by `dbt_utils.generate_surrogate_key()`. Their values depend on the hash function, which varies by adapter (MD5 on most, different on BigQuery).

**Two approaches:**

1. **Pre-compute the hashes** and put them in the expected CSV. This is brittle — if the hash function changes, all expected CSVs break.

2. **Exclude surrogate keys from comparison.** Use the `compare_columns` parameter of `dbt_utils.equality` to list only the non-key columns. This is more robust but means you're not testing key generation.

**Recommended:** Exclude surrogate keys from the equality comparison. Test key uniqueness separately with `dbt_utils.unique_combination_of_columns` on the actual model.

```yaml
- name: basic_id_changes_actual
  tests:
    - dbt_utils.equal_rowcount:
        compare_model: ref('basic_id_changes_expected_stg')
    - dbt_utils.equality:
        compare_model: ref('basic_id_changes_expected_stg')
        compare_columns:
          - snowplow_id
          - previous_snowplow_id
          - effective_at
          - change_type
          - first_seen_event_id
          - first_seen_app_id
```

---

## Reference: JSON structures for seed data

### Identity context column

Column name: `contexts_com_snowplowanalytics_snowplow_identity_1`

```json
[{"snowplowId": "sp_A", "createdAt": "2026-03-10T08:00:00"}]
```

- Always an array (even for a single identity)
- `snowplowId`: the Snowplow identity ID (string)
- `createdAt`: ISO timestamp when the identity was created

### Merge event column

Column name: `unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1`

```json
{
  "snowplowId": "sp_A",
  "merged": [
    {"snowplowId": "sp_B", "mergedAt": "2026-03-10T09:00:00", "triggeringEventId": "evt-7"}
  ],
  "merges": ["sp_B"]
}
```

- Single object (not array) — this is an unstruct_event, not a context
- `snowplowId`: the parent (survivor) identity
- `merged`: array of child identities being absorbed
  - `snowplowId`: the child identity ID
  - `mergedAt`: ISO timestamp of the merge
  - `triggeringEventId`: the event_id that triggered the merge
- `merges`: array of child identity IDs (string array, same IDs as in `merged`)

### Multi-child merge

```json
{
  "snowplowId": "sp_A",
  "merged": [
    {"snowplowId": "sp_B", "mergedAt": "2026-03-10T09:00:00", "triggeringEventId": "evt-7"},
    {"snowplowId": "sp_C", "mergedAt": "2026-03-10T09:00:00", "triggeringEventId": "evt-7"}
  ],
  "merges": ["sp_B", "sp_C"]
}
```

### NULL context (event without identity)

Leave the column empty in the CSV (no value between commas). dbt will load it as NULL.

---

## Reference: minimum atomic.events columns

The package reads from `base_events_this_run` which is provided by `snowplow_utils`. The minimum columns the identities package actually uses:

| Column | Used by | Required |
|---|---|---|
| `event_id` | Dedup, surrogate keys | Yes |
| `event_name` | Filter `identity_merge` events | Yes |
| `app_id` | `first_app_id`, `last_app_id` | Yes |
| `collector_tstamp` | Dedup tie-breaking | Yes |
| `derived_tstamp` | First/last timestamps | Yes |
| `load_tstamp` | Incremental manifest slicing | Yes |
| `domain_userid` | Identifier mapping (configurable) | Yes |
| `user_id` | Identifier mapping (configurable) | Yes |
| `contexts_com_snowplowanalytics_snowplow_identity_1` | Identity context extraction | Yes (NULL for non-identity events) |
| `unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1` | Merge event extraction | Yes (NULL for non-merge events) |

Other `atomic.events` columns (geo, referer, marketing, etc.) can be NULL in seed data. Include them in the CSV header with empty values to match the schema that `snowplow_utils.base_create_snowplow_events_this_run()` expects.
