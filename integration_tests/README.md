# <YOUR PACKAGE NAME>-integration-tests

Integration test suite for the <YOUR PACKAGE NAME> dbt package.

The `./scripts` directory contains the following:

- `integration_tests.sh`: This tests the standard modules of the <YOUR PACKAGE NAME> package:

- Runs the <YOUR PACKAGE NAME> package 4 times to replicate incremental loading of events, then performs an equality test between the actual vs expected output.

Run the scripts using:

```bash
bash integration_tests.sh -d {warehouse}
```

Supported warehouses (should be the same as your target in your profile.yml):

- redshift
- bigquery
- snowflake
- databricks
- postgres
- all (iterates through all supported warehouses)

### What to look out for when writing integration tests:

There are certain exceptions to how different warehouses process data and in places you may need to adjust the integration tests to work around that. Here's a list of things to keep in mind:

- the non-deterministic nature of the `row_number()` function for Redshift/Postgres/Databricks means that you may need to hard-code actuals and expected models for cases where we are testing duplicate rows with exact same results / window
- BigQuery handling of snowplow_utils.timestamp_diff() can sometimes round values differently to the other warehouses.
