{% docs __snowplow_identities__ %}

{% raw %}

# snowplow-identities package

Welcome to the model documentation site for the snowplow-identities dbt package.

**For more information, including a QuickStart guide, operation and configuration, and implementing your own custom modules on top of this please visit the [Snowplow Docs](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/).**

*Note this model design doc site is linked to latest release of the package. If you are not using the latest release, [generate and serve](https://docs.getdbt.com/reference/commands/cmd-docs#dbt-docs-serve) the doc site locally for accurate documentation.*

## Overview

The package resolves Snowplow identities into a single view of a person, in two layers.

**Staging** reads `atomic.events` directly:

- `snowplow_identities_stg_identity_events`. One row per event carrying the identity context, with the configured identifier columns kept un-pivoted.
- `snowplow_identities_merge_events`. An append-only log of every `identity_merge` event, and also a derived output, since it is the only place the raw merge payloads are retained.

**Derived** is the customer surface:

- `snowplow_identities_snowplow_id_mapping`. Current child to active-parent mapping. Join your events' `snowplow_id` here to resolve them to one unified identity.
- `snowplow_identities_identifier_mapping`. A view listing every external identifier against its current `active_snowplow_id`. Use this to look up a person by email or user ID for activation downstream.
- `snowplow_identities_identities`. One row per identity, with when it was created and where it was first and last seen.

Two design choices are worth knowing about.

*Resolution happens at read time.* `identifier_mapping` is a view over `snowplow_identities_identifier_mapping_base`, whose rows are keyed on an immutable surrogate and never rewritten. A merge is reflected the moment `snowplow_id_mapping` updates, and an identifier that expires and later reappears under a new identity keeps both records.

*Each model tracks its own progress.* Every model asks its own table for the newest `load_tstamp` it holds and processes forward from there, in windows of at least `snowplow__backfill_limit_days`. There is no manifest and no run hooks, so a failed run leaves the affected model slightly behind and it catches up on the next one. Models whose events are sparse, such as `merge_events`, extend their window to `snowplow_identities_stg_identity_events`' watermark so an empty window cannot strand them. Each run logs the window it is processing.

## Installation

Check [dbt Hub](https://hub.getdbt.com/snowplow/snowplow_identities/latest/) for the latest installation instructions, or read the [dbt docs][dbt-package-docs] for more information on installing packages.

# Join the Snowplow community

We welcome all ideas, questions and contributions!
If you find a bug, please report an issue on GitHub.

# Copyright and license

The snowplow-identities package is Copyright 2026-present Snowplow Analytics Ltd.

Licensed under the [Snowplow Personal and Academic License][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[license]: https://docs.snowplow.io/personal-and-academic-license-1.0/
[license-image]: http://img.shields.io/badge/license-Snowplow--Personal--and--Academic--1-blue.svg?style=flat
[tracker-classificiation]: https://docs.snowplow.io/docs/collecting-data/collecting-from-own-applications/tracker-maintenance-classification/
[early-release]: https://img.shields.io/static/v1?style=flat&label=Snowplow&message=Early%20Release&color=014477&labelColor=9ba0aa&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAMAAAAoLQ9TAAAAeFBMVEVMaXGXANeYANeXANZbAJmXANeUANSQAM+XANeMAMpaAJhZAJeZANiXANaXANaOAM2WANVnAKWXANZ9ALtmAKVaAJmXANZaAJlXAJZdAJxaAJlZAJdbAJlbAJmQAM+UANKZANhhAJ+EAL+BAL9oAKZnAKVjAKF1ALNBd8J1AAAAKHRSTlMAa1hWXyteBTQJIEwRgUh2JjJon21wcBgNfmc+JlOBQjwezWF2l5dXzkW3/wAAAHpJREFUeNokhQOCA1EAxTL85hi7dXv/E5YPCYBq5DeN4pcqV1XbtW/xTVMIMAZE0cBHEaZhBmIQwCFofeprPUHqjmD/+7peztd62dWQRkvrQayXkn01f/gWp2CrxfjY7rcZ5V7DEMDQgmEozFpZqLUYDsNwOqbnMLwPAJEwCopZxKttAAAAAElFTkSuQmCC

[dbt-package-docs]: https://docs.getdbt.com/docs/building-a-dbt-project/package-management
[snowplow-utils]: https://github.com/snowplow/dbt-snowplow-utils
{% endraw %}
{% enddocs %}
