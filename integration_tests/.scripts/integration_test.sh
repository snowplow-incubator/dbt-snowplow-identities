#!/bin/bash

# Expected input:
# -d (database) target database for dbt

while getopts 'd:' opt
do
  case $opt in
    d) DATABASE=$OPTARG
  esac
done

declare -a SUPPORTED_DATABASES=("bigquery" "snowflake")

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

  # Leg 1: snowplow__merge_limit_collapse off (the default). The merge-limit
  # scenarios keep their duplicate rows here, so the collapse-expectation
  # tests are excluded.
  echo "snowplow-identities integration tests: Execute models - run 1/4 (full refresh)"
  eval "dbt run --full-refresh --vars '{snowplow__allow_refresh: true, snowplow__backfill_limit_days: 1}' --target $db" || exit 1;

  for i in {2..4}
  do
    echo "snowplow-identities integration tests: Execute models - run $i/4"
    eval "dbt run --target $db" || exit 1;
  done

  echo "snowplow-identities integration tests: Test models (merge limit collapse off)"
  if [[ $db == "bigquery" ]]; then
    eval "dbt test --exclude tag:snowflake_only tag:merge_limit_collapse --target $db" || exit 1;
  else
    eval "dbt test --exclude tag:merge_limit_collapse --target $db" || exit 1;
  fi

  # Leg 2: snowplow__merge_limit_collapse on. The merge-limit scenarios
  # collapse to one row per identifier here, so the default-expectation
  # tests for identifier_mapping are excluded.
  echo "snowplow-identities integration tests: Execute models with merge limit collapse - run 1/4 (full refresh)"
  eval "dbt run --full-refresh --vars '{snowplow__merge_limit_collapse: true, snowplow__allow_refresh: true, snowplow__backfill_limit_days: 1}' --target $db" || exit 1;

  for i in {2..4}
  do
    echo "snowplow-identities integration tests: Execute models with merge limit collapse - run $i/4"
    eval "dbt run --vars '{snowplow__merge_limit_collapse: true}' --target $db" || exit 1;
  done

  echo "snowplow-identities integration tests: Test models (merge limit collapse on)"
  if [[ $db == "bigquery" ]]; then
    eval "dbt test --vars '{snowplow__merge_limit_collapse: true}' --exclude tag:snowflake_only tag:no_merge_limit_collapse --target $db" || exit 1;
  else
    eval "dbt test --vars '{snowplow__merge_limit_collapse: true}' --exclude tag:no_merge_limit_collapse --target $db" || exit 1;
  fi

  echo "snowplow-identities integration tests: All tests passed"

done
