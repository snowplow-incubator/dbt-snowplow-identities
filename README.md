[![early-release]][tracker-classification]
[![Release][release-image]][releases]
[![License][license-image]][license]
![snowplow-logo](https://raw.githubusercontent.com/snowplow/dbt-snowplow-utils/main/assets/snowplow_logo.png)

# snowplow-identities

A dbt package that resolves Snowplow identities. It reads the identity context and
`identity_merge` events from `atomic.events`, then keeps track of which Snowplow IDs belong to
the same person after merges, and which of your own identifiers point at them.

## Models

| Model | Description |
|-------|-------------|
| `snowplow_identities_snowplow_id_mapping` | Each child `snowplow_id` and the active parent it now resolves to. Join your events here to get one ID per person. |
| `snowplow_identities_identifier_mapping` | Your identifiers, such as `domain_userid` and `user_id`, against the `active_snowplow_id` that currently owns them. A view, resolved at read time. |
| `snowplow_identities_identities` | One row per `snowplow_id`, with when it was created and where it was first and last seen. |
| `snowplow_identities_merge_events` | The `identity_merge` event log, keeping the raw merge payloads. |

Two more models support these. `snowplow_identities_identifier_mapping_base` is the table the
`identifier_mapping` view resolves against, and `snowplow_identities_stg_identity_events` holds
the identity context events read from `atomic.events`.

Please refer to the [doc site](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-models/dbt-identities-data-model/) for a full breakdown of the package.

Please note that this data model is under the Snowplow Personal & Academic License (SPAL). For
further details please refer to our [documentation site](https://docs.snowplow.io/docs/contributing/personal-and-academic-license-faq/).

### Getting started

The easiest way to get started is to follow our [QuickStart guide](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-quickstart/identities/).

Upgrading from 0.1.0 is a breaking change. See [MIGRATION.md](MIGRATION.md) for the steps and
for recipes rebuilding the tables this release removes.

### Adapter support

BigQuery and Snowflake.

### Requirements

- Events carrying the `com.snowplowanalytics.snowplow/identity` context, version 2, available
  in your warehouse.
- dbt-core 1.10.6 or greater.
- `snowplow_utils` 1.0.1 or greater, which `dbt deps` installs for you.

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
