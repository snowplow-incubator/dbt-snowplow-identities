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

  echo "snowplow-identities integration tests: Execute models - run 1/4 (full refresh)"
  eval "dbt run --full-refresh --vars '{snowplow__allow_refresh: true, snowplow__backfill_limit_days: 1}' --target $db" || exit 1;

  for i in {2..4}
  do
    echo "snowplow-identities integration tests: Execute models - run $i/4"
    eval "dbt run --target $db" || exit 1;
  done

  echo "snowplow-identities integration tests: Test models"
  if [[ $db == "bigquery" ]]; then
    eval "dbt test --exclude tag:snowflake_only --target $db" || exit 1;
  else
    eval "dbt test --target $db" || exit 1;
  fi

  echo "snowplow-identities integration tests: All tests passed"

done
