#!/bin/bash

# Expected input:
# -d (database) target database for dbt

while getopts 'd:' opt
do
  case $opt in
    d) DATABASE=$OPTARG
  esac
done

declare -a SUPPORTED_DATABASES=("bigquery" "snowflake" "databricks")

# set to lower case
DATABASE="$(echo $DATABASE | tr '[:upper:]' '[:lower:]')"

if [[ $DATABASE == "all" ]]; then
  DATABASES=( "${SUPPORTED_DATABASES[@]}" )
else
  DATABASES=$DATABASE
fi

for db in ${DATABASES[@]}; do

  echo "snowplow-identities integration tests: Seeding data"
  eval "dbt seed --full-refresh --target $db" || exit 1;

  # Package models read atomic.events as a source, so dbt cannot infer a DAG edge
  # to this integration-only staging model. Build it before the package runs.
  dbt run --select snowplow_identities_events_stg --target "$db" || exit 1

  # Leg 1: retain every immutable mapping, without selecting a preferred owner.
  echo "snowplow-identities integration tests: Execute models - run 1/4 (full refresh)"
  eval "dbt run --exclude snowplow_identities_events_stg --full-refresh --vars '{snowplow__backfill_limit_days: 1}' --target $db" || exit 1;

  for i in {2..4}
  do
    echo "snowplow-identities integration tests: Execute models - run $i/4"
    eval "dbt run --exclude snowplow_identities_events_stg --target $db" || exit 1;
  done

  echo "snowplow-identities integration tests: Test models (merge limit collapse off)"
  dbt test --target "$db" || exit 1

  # Leg 2: keep the same rows and validate the opt-in preferred-owner flags.
  echo "snowplow-identities integration tests: Execute models with merge limit collapse - run 1/4 (full refresh)"
  eval "dbt run --exclude snowplow_identities_events_stg --full-refresh --vars '{snowplow__merge_limit_collapse: true, snowplow__backfill_limit_days: 1}' --target $db" || exit 1;

  for i in {2..4}
  do
    echo "snowplow-identities integration tests: Execute models with merge limit collapse - run $i/4"
    eval "dbt run --exclude snowplow_identities_events_stg --vars '{snowplow__merge_limit_collapse: true}' --target $db" || exit 1;
  done

  echo "snowplow-identities integration tests: Test models (merge limit collapse on)"
  dbt test --vars '{snowplow__merge_limit_collapse: true}' --target "$db" || exit 1

  echo "snowplow-identities integration tests: All tests passed"

done
