#!/usr/bin/env bash
set -euo pipefail

warehouse=$1
baseline_project=$2
candidate_project=$(pwd)
export DBT_PROFILES_DIR="$candidate_project/ci"
# The normal CI cleanup also removes this suffix, even if the upgrade fails.
export SCHEMA_SUFFIX="${SCHEMA_SUFFIX}_upgrade"

cd "$baseline_project"
dbt deps
dbt seed --full-refresh --target "$warehouse"
dbt run --full-refresh --vars '{snowplow__backfill_limit_days: 4, snowplow__allow_refresh: true}' --target "$warehouse"

cd "$candidate_project"
# Follow MIGRATION.md's repartitioning step; leave identifier_mapping as a real
# populated 0.1.0 table so the table-to-view conversion is exercised by dbt.
dbt run-operation prepare_upgrade_fixture --target "$warehouse"
dbt seed --full-refresh --target "$warehouse"
dbt run --select snowplow_identities_events_stg --target "$warehouse"
dbt run --exclude snowplow_identities_events_stg --vars '{snowplow__backfill_limit_days: 4}' --target "$warehouse"
dbt test --target "$warehouse"

# A second run must retain the same output rows, including TTL ownership.
dbt run --exclude snowplow_identities_events_stg --vars '{snowplow__backfill_limit_days: 4}' --target "$warehouse"
dbt test --select test_type:data --target "$warehouse"
