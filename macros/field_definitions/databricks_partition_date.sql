{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{#
  Emits a leading-comma `cast(<expression> as date) as <alias>_date` projection
  when the target is Databricks or Spark, else nothing. The `partition_by`
  configs on incremental models reference these `<alias>_date` columns; this
  macro is the only thing that materialises them.

  Pass `alias` separately when `tstamp_expression` is anything other than a
  bare column name (e.g. a CASE or LEAST expression) — otherwise the alias
  defaults to the expression itself.
#}
{% macro databricks_partition_date(tstamp_expression, alias=none) -%}
  {%- if target.type in ['databricks', 'spark'] -%}
  {%- set column_alias = alias if alias is not none else tstamp_expression -%}
  , cast({{ tstamp_expression }} as date) as {{ column_alias }}_date
  {%- endif -%}
{%- endmacro %}
