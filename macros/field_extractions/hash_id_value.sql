{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{#
  Return a hex-encoded SHA-256 of a lower-trimmed string. BigQuery's sha256()
  returns BYTES so it needs to_hex(); Snowflake and Databricks both have
  sha2(string, 256) that returns hex directly.
#}
{% macro hash_id_value(column_expression) %}
  {{ return(adapter.dispatch('hash_id_value', 'snowplow_identities')(column_expression)) }}
{% endmacro %}

{% macro default__hash_id_value(column_expression) %}
  {{ exceptions.raise_compiler_error(
    "The `hash_id_value` macro is not implemented for adapter `"
    ~ target.type
    ~ "`. Supported adapters are: `bigquery`, `snowflake`, `databricks`, `spark`."
  ) }}
{% endmacro %}

{% macro bigquery__hash_id_value(column_expression) %}
  to_hex(sha256(lower(trim({{ column_expression }}))))
{% endmacro %}

{% macro snowflake__hash_id_value(column_expression) %}
  sha2(lower(trim({{ column_expression }})), 256)
{% endmacro %}

{# Databricks dispatches to spark__ via the dbt-databricks → dbt-spark adapter chain. #}
{% macro spark__hash_id_value(column_expression) %}
  sha2(lower(trim({{ column_expression }})), 256)
{% endmacro %}
