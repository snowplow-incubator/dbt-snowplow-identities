{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{#
  Aggregate. True when the expression is true for any row in the group. Every dialect's
  native aggregate ignores null rows and returns null for a group of nothing but nulls,
  so feed it an expression that can never be null. reason_contains already coalesces, so
  any combination of its calls qualifies.
#}

{% macro bool_or_agg(expr) %}
  {{ return(adapter.dispatch('bool_or_agg', 'snowplow_identities')(expr)) }}
{%- endmacro -%}

{% macro snowflake__bool_or_agg(expr) %}
  boolor_agg({{ expr }})
{%- endmacro -%}

{% macro bigquery__bool_or_agg(expr) %}
  logical_or({{ expr }})
{%- endmacro -%}
