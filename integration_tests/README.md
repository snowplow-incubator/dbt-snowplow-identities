# snowplow-identities integration tests

Integration test suite for the snowplow-identities dbt package.

## Overview

The tests validate the full identity resolution pipeline end-to-end by:

1. Seeding 76 source events across 12 scenario groups
2. Running dbt 4 times (1 full-refresh + 3 incremental) to simulate batch processing
3. Comparing 6 derived output tables against pre-computed expected results

## Running Tests

```bash
bash .scripts/integration_test.sh -d {warehouse}
```

Supported warehouses:
- `snowflake`
- `bigquery`
- `all` (iterates through all supported warehouses)

## Batch Strategy

Events are spread across 4 batches via `load_tstamp` windows:

| Batch | Date | Type | Events |
|-------|------|------|--------|
| 1 | 2026-01-01 | full-refresh | 52 |
| 2 | 2026-01-02 | incremental | 18 |
| 3 | 2026-01-03 | incremental | 5 |
| 4 | 2026-01-04 | incremental | 1 |

## ID Naming Convention

All test data uses self-documenting IDs with group prefixes:

- Snowplow IDs: `sp_<GROUP>_<LETTER>` (e.g., `sp_CHAIN_A`)
- Domain user IDs: `duid_<GROUP>_<LETTER>` (e.g., `duid_CHAIN_A`)
- User IDs: `uid_<GROUP>_<LETTER>` (e.g., `uid_SIMPLE_A`)
- Event IDs: `evt_<GROUP>_<NN>` (e.g., `evt_CHAIN_03`)

## Scenario Groups

See [SCENARIOS.md](SCENARIOS.md) for the complete registry mapping each group to the unit tests it covers.

| Group | Story | Batches |
|-------|-------|---------|
| SIMPLE | Basic create + merge | 1, 4 |
| CHAIN | Transitive chain + repointing | 1, 2, 3 |
| FANIN | Fan-in merge + repointing | 1, 2 |
| FANOUT | Multi-child single event + history resolution | 1, 2 |
| DIAMOND | Diamond topology | 1, 2 |
| DISJOINT | Isolated groups | 1, 2 |
| LONGCHAIN | Deep chain within batch | 1 |
| ALTCHAIN | Alternating history/new chain | 1, 2 |
| MERGEONLY | Merge without creation in same batch | 1, 2 |
| MULTIID | Multiple identifier types + cross-app | 1, 3 |
| LATE | Late-arriving data | 1, 3 |
| REMAP | Child re-merged to different parent | 1, 3 |

## Adding New Scenarios

1. Choose a short prefix (e.g., `NEWSCN`)
2. Add events to `data/source/snowplow_identities_events.csv`
3. Update all 6 expected CSVs in `data/expected/`
4. Add a section to `SCENARIOS.md`

## Validated Tables

After all 4 runs, these derived tables are compared against expected seeds:

- `snowplow_identities_snowplow_id_mapping`
- `snowplow_identities_id_changes`
- `snowplow_identities_identifier_mapping`
- `snowplow_identities_id_mapping_scd`
- `snowplow_identities_new_identities`
- `snowplow_identities_merge_events`
