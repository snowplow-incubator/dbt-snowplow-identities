{% docs __snowplow_identities__ %}

{% raw %}

# snowplow-identities package

Welcome to the model documentation site for the snowplow-identities dbt package.

**For more information, including a QuickStart guide, operation and configuration, and implementing your own custom modules on top of this please visit the [Snowplow Docs](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/).**

*Note this model design doc site is built from the main branch of the package. If you are not using the latest release, [generate and serve](https://docs.getdbt.com/reference/commands/cmd-docs#dbt-docs-serve) the doc site locally for accurate documentation.*

## Overview

This model consists of a series of modules, each producing a table which serves as the input to the next module. The 'standard' modules are:

- Base: Performs the incremental logic, outputting the table `snowplow_identities_base_events_this_run` which contains a de-duped data set of all events required for the current run of the model.

Each module produces a table which acts as the input to the subsequent module (the `_this_run` tables), and updates a derived table - with the exception of the Base module, which takes atomic data as its input, and does not update a derived table.

## Installation

Install the tagged release in `packages.yml`, then run `dbt deps`:

```yaml
packages:
  - git: "https://github.com/snowplow-incubator/dbt-snowplow-identities.git"
    revision: "0.1.0"
```

See the [dbt package documentation][dbt-package-docs] for more information.

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
