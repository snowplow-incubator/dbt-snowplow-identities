[![early-release]][tracker-classification]
[![Release][release-image]][releases]
[![License][license-image]][license]
![snowplow-logo](https://raw.githubusercontent.com/snowplow/dbt-snowplow-utils/main/assets/snowplow_logo.png)

# snowplow-identities

A dbt package for building robust incremental identity resolution models from Snowplow data. Processes identity merge events to maintain mappings between user identifiers and canonical Snowplow IDs, with full audit history of identity lifecycle changes.

## Features

- **Identity Resolution**: Maps all child snowplow_ids to their current active parent after merges
- **Identifier Mapping**: Links external identifiers (domain_userid, user_id, etc.) to canonical Snowplow IDs
- **Audit Trail**: Complete history of identity creation and merge events
- **Incremental Processing**: Efficient delta updates using Snowplow's incremental framework
- **Multi-Warehouse**: Adapter-specific SQL for BigQuery, Snowflake and Databricks

## Core Models

| Model | Description |
|-------|-------------|
| `snowplow_identities_snowplow_id_mapping` | Current child → parent ID mappings |
| `snowplow_identities_snowplow_id_mapping_snapshot` | SCD Type 2 history of ID mapping changes with valid_from/valid_to timestamps |
| `snowplow_identities_identifier_mapping` | External identifiers → active Snowplow ID lookup |
| `snowplow_identities_id_changes` | Audit log of all identity lifecycle events |
| `snowplow_identities_merge_events` | History of identity merge operations |


Please refer to the [doc site](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-models/dbt-identities-data-model/) for a full breakdown of the package.

Please note that this data model is under the Snowplow Personal & Academic License (SPAL). For further details please refer to our [documenation site](https://docs.snowplow.io/docs/contributing/personal-and-academic-license-faq/).

### Getting Started

The easiest way to get started is to follow our [QuickStart guide](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-quickstart/identities/).

### Adapter Support

The package includes adapters for BigQuery, Snowflake and Databricks.

For Databricks, use `dbt-databricks` and a Databricks SQL warehouse or Runtime
12.2 LTS or later (the models use `UNPIVOT`). The integration test adapter is
pinned to `dbt-databricks==1.11.8`. Identity entities must be typed arrays of
structs and merge events must be typed structs, as produced by the Snowplow
Databricks loader. Incremental output tables use Delta and date partitions.

Set `snowplow__databricks_catalog` to the catalog containing the source events
(default: `hive_metastore`), and `snowplow__atomic_schema` to their schema.
Configure the destination catalog and schema in the dbt Databricks profile.
The `spark__` macros are shared through adapter dispatch; standalone Spark is
not covered by this package's warehouse integration suite.

### Requirements

- A dataset of web events from the [Snowplow JavaScript tracker][tracker-docs] must be available in the database.
- Have the [`identity` context][identity-context] enabled.
- dbt-core version 1.10.6 or greater, below 2.0.0

# Copyright and license

The snowplow-identities package is Copyright 2026-present Snowplow Analytics Ltd.

This distribution is all licensed under the [Snowplow Personal and Academic License][license]. (If you are uncertain how it applies to your use case, check our answers to [frequently asked questions](https://docs.snowplow.io/docs/contributing/community-license-faq/).)

[license]: https://docs.snowplow.io/personal-and-academic-license-1.0/
[license-image]: http://img.shields.io/badge/license-Snowplow--Personal--and--Academic--1-blue.svg?style=flat

[website]: https://snowplow.io/
[snowplow]: https://github.com/snowplow/snowplow
[docs]: https://docs.snowplow.io/

[release-image]: https://img.shields.io/github/v/release/snowplow/dbt-snowplow-identities?sort=semver
[releases]: https://github.com/snowplow/dbt-snowplow-identities/releases

[tracker-classification]: https://docs.snowplow.io/docs/collecting-data/collecting-from-own-applications/tracker-maintenance-classification/
[early-release]: https://img.shields.io/static/v1?style=flat&label=Snowplow&message=Early%20Release&color=014477&labelColor=9ba0aa&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAMAAAAoLQ9TAAAAeFBMVEVMaXGXANeYANeXANZbAJmXANeUANSQAM+XANeMAMpaAJhZAJeZANiXANaXANaOAM2WANVnAKWXANZ9ALtmAKVaAJmXANZaAJlXAJZdAJxaAJlZAJdbAJlbAJmQAM+UANKZANhhAJ+EAL+BAL9oAKZnAKVjAKF1ALNBd8J1AAAAKHRSTlMAa1hWXyteBTQJIEwRgUh2JjJon21wcBgNfmc+JlOBQjwezWF2l5dXzkW3/wAAAHpJREFUeNokhQOCA1EAxTL85hi7dXv/E5YPCYBq5DeN4pcqV1XbtW/xTVMIMAZE0cBHEaZhBmIQwCFofeprPUHqjmD/+7peztd62dWQRkvrQayXkn01f/gWp2CrxfjY7rcZ5V7DEMDQgmEozFpZqLUYDsNwOqbnMLwPAJEwCopZxKttAAAAAElFTkSuQmCC
[unsupported]: https://img.shields.io/static/v1?style=flat&label=Snowplow&message=Unsupported&color=24292e&labelColor=lightgrey&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAMAAAAoLQ9TAAAAeFBMVEVMaXGXANeYANeXANZbAJmXANeUANSQAM+XANeMAMpaAJhZAJeZANiXANaXANaOAM2WANVnAKWXANZ9ALtmAKVaAJmXANZaAJlXAJZdAJxaAJlZAJdbAJlbAJmQAM+UANKZANhhAJ+EAL+BAL9oAKZnAKVjAKF1ALNBd8J1AAAAKHRSTlMAa1hWXyteBTQJIEwRgUh2JjJon21wcBgNfmc+JlOBQjwezWF2l5dXzkW3/wAAAHpJREFUeNokhQOCA1EAxTL85hi7dXv/E5YPCYBq5DeN4pcqV1XbtW/xTVMIMAZE0cBHEaZhBmIQwCFofeprPUHqjmD/+7peztd62dWQRkvrQayXkn01f/gWp2CrxfjY7rcZ5V7DEMDQgmEozFpZqLUYDsNwOqbnMLwPAJEwCopZxKttAAAAAElFTkSuQmCC
[maintained]: https://img.shields.io/static/v1?style=flat&label=Snowplow&message=Maintained&color=9e62dd&labelColor=9ba0aa&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAMAAAAoLQ9TAAAAeFBMVEVMaXGXANeYANeXANZbAJmXANeUANSQAM+XANeMAMpaAJhZAJeZANiXANaXANaOAM2WANVnAKWXANZ9ALtmAKVaAJmXANZaAJlXAJZdAJxaAJlZAJdbAJlbAJmQAM+UANKZANhhAJ+EAL+BAL9oAKZnAKVjAKF1ALNBd8J1AAAAKHRSTlMAa1hWXyteBTQJIEwRgUh2JjJon21wcBgNfmc+JlOBQjwezWF2l5dXzkW3/wAAAHpJREFUeNokhQOCA1EAxTL85hi7dXv/E5YPCYBq5DeN4pcqV1XbtW/xTVMIMAZE0cBHEaZhBmIQwCFofeprPUHqjmD/+7peztd62dWQRkvrQayXkn01f/gWp2CrxfjY7rcZ5V7DEMDQgmEozFpZqLUYDsNwOqbnMLwPAJEwCopZxKttAAAAAElFTkSuQmCC
[actively-maintained]: https://img.shields.io/static/v1?style=flat&label=Snowplow&message=Actively%20Maintained&color=6638b8&labelColor=9ba0aa&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAMAAAAoLQ9TAAAAeFBMVEVMaXGXANeYANeXANZbAJmXANeUANSQAM+XANeMAMpaAJhZAJeZANiXANaXANaOAM2WANVnAKWXANZ9ALtmAKVaAJmXANZaAJlXAJZdAJxaAJlZAJdbAJlbAJmQAM+UANKZANhhAJ+EAL+BAL9oAKZnAKVjAKF1ALNBd8J1AAAAKHRSTlMAa1hWXyteBTQJIEwRgUh2JjJon21wcBgNfmc+JlOBQjwezWF2l5dXzkW3/wAAAHpJREFUeNokhQOCA1EAxTL85hi7dXv/E5YPCYBq5DeN4pcqV1XbtW/xTVMIMAZE0cBHEaZhBmIQwCFofeprPUHqjmD/+7peztd62dWQRkvrQayXkn01f/gWp2CrxfjY7rcZ5V7DEMDQgmEozFpZqLUYDsNwOqbnMLwPAJEwCopZxKttAAAAAElFTkSuQmCC
