#!/bin/bash

# Expected input:
# -d (database) target database for dbt

while getopts 'd:' opt
do
  case $opt in
    d) DATABASE=$OPTARG
  esac
done

declare -a SUPPORTED_DATABASES=("bigquery" "postgres" "databricks" "redshift" "snowflake")

# set to lower case
DATABASE="$(echo $DATABASE | tr '[:upper:]' '[:lower:]')"

if [[ $DATABASE == "all" ]]; then
  DATABASES=( "${SUPPORTED_DATABASES[@]}" )
else
  DATABASES=$DATABASE
fi

for db in ${DATABASES[@]}; do

  echo "<YOUR PACKAGE NAME> integration tests: Seeding data"
  eval "dbt seed --full-refresh --target $db" || exit 1;

  echo "<YOUR PACKAGE NAME> integration tests: Try run without data"
  eval "dbt run --full-refresh --vars '{snowplow__allow_refresh: true, snowplow__backfill_limit_days: 1, snowplow__start_date: 2010-01-01}' --target $db" || exit 1;

  echo "<YOUR PACKAGE NAME> integration tests: Execute models - run 1/4"
  eval "dbt run --full-refresh --vars '{snowplow__allow_refresh: true, snowplow__backfill_limit_days: 243}' --target $db" || exit 1;

  for i in {2..4}
  do
    echo "<YOUR PACKAGE NAME> integration tests: Execute models - run $i/4"
    eval "dbt run --target $db" || exit 1;
  done

  echo "<YOUR PACKAGE NAME> integration tests: Test models"

  eval "dbt test test_name:not_null --store-failures --target $db" || exit 1;

  echo "<YOUR PACKAGE NAME> integration tests: All tests passed"

done
