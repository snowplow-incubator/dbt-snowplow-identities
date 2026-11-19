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
# Keep populated 0.1.0 tables in place. dbt must repartition the incremental
# tables and convert identifier_mapping to a view during the initial full refresh.
dbt run-operation check_upgrade_fixture --target "$warehouse"
if dbt run --select package:snowplow_identities --target "$warehouse" > upgrade_guard.log 2>&1; then
  echo 'An upgrade without full refresh unexpectedly succeeded'
  exit 1
fi
grep -F 'Upgrading snowplow_identities from 0.1.0 requires a package-scoped --full-refresh' upgrade_guard.log
dbt run-operation check_upgrade_fixture --target "$warehouse"
if dbt run --select snowplow_identities_merge_events --full-refresh --target "$warehouse" > upgrade_partial_guard.log 2>&1; then
  echo 'A partial full refresh of the legacy package unexpectedly succeeded'
  exit 1
fi
grep -F 'Include every enabled package model' upgrade_partial_guard.log
dbt run-operation check_upgrade_fixture --target "$warehouse"
dbt seed --full-refresh --target "$warehouse"
# Selecting only the consuming project's source stage must remain possible while
# the package tables still have their old schema. No full-refresh bypass is used.
dbt run --select snowplow_identities_events_stg --target "$warehouse"
dbt run-operation check_upgrade_fixture --target "$warehouse"
dbt run --full-refresh --exclude snowplow_identities_events_stg --vars '{snowplow__backfill_limit_days: 4}' --target "$warehouse"
dbt test --target "$warehouse"

# A second run must retain the same output rows, including TTL ownership.
dbt run --exclude snowplow_identities_events_stg --vars '{snowplow__backfill_limit_days: 4}' --target "$warehouse"
dbt test --select test_type:data --target "$warehouse"
