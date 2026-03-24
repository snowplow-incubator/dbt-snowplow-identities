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

  echo "Integration tests: Seeding data"
  eval "dbt seed --full-refresh --target $db" || exit 1

  echo "Integration tests: Run with empty tables"
  eval "dbt run --full-refresh --vars '{snowplow__allow_refresh: true, snowplow__backfill_limit_days: 1, snowplow__start_date: 2010-01-01, test_group: basic}' --target $db" || exit 1

  for group in basic complex_merges edge_cases; do

    echo "Integration tests: Running group $group"

    echo "Integration tests: Resetting manifest for $group"
    eval "dbt run --select snowplow_identities_incremental_manifest --full-refresh --vars '{snowplow__allow_refresh: true, test_group: $group}' --target $db" || exit 1

    echo "Integration tests: Run 1/4 (full-refresh) for $group"
    eval "dbt run --full-refresh --vars '{snowplow__allow_refresh: true, snowplow__backfill_limit_days: 1, test_group: $group}' --target $db" || exit 1

    for i in {2..4}; do
      echo "Integration tests: Run $i/4 (incremental) for $group"
      eval "dbt run --vars '{snowplow__backfill_limit_days: 1, test_group: $group}' --target $db" || exit 1
    done

    echo "Integration tests: Testing group $group"
    eval "dbt test --select tag:$group --vars '{store_failures: true}' --target $db" || exit 1

    echo "Integration tests: Group $group passed on $db"

  done

  echo "Integration tests: Testing uniqueness constraints"
  eval "dbt test --select 'test_name:unique test_name:unique_combination_of_columns' --vars '{store_failures: true}' --target $db" || exit 1

  echo "Integration tests: All tests passed on $db"

done
