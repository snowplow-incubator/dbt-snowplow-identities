{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{#
  Set operations over the string arrays the identity context carries, decision_reasons
  being the only one today. The value is a set, so every macro here returns it sorted,
  deduplicated and never null: unsorted output would make a stored array depend on the
  order events happened to arrive in, which no expectation could pin down.

  array_concat_agg is the exception, and the reason there are three macros rather than one
  aggregate: BigQuery cannot aggregate arrays and deduplicate them in the same select, so a
  group union is array_concat_agg in one CTE and array_sort_distinct in the next.
#}

{% macro array_union(a, b) %}
  {{ return(adapter.dispatch('array_union', 'snowplow_identities')(a, b)) }}
{%- endmacro -%}

{% macro snowflake__array_union(a, b) %}
  array_sort(array_distinct(array_cat(coalesce({{ a }}, array_construct()), coalesce({{ b }}, array_construct()))))
{%- endmacro -%}

{% macro bigquery__array_union(a, b) %}
  array(select distinct x from unnest(array_concat(ifnull({{ a }}, []), ifnull({{ b }}, []))) as x order by x)
{%- endmacro -%}

{#
  The array type to cast a null to when the stored side of a union is not there yet. A
  bare null has no type on BigQuery, so the concat it feeds cannot resolve its element
  type without one.
#}

{% macro reason_array_type() %}
  {{ return(adapter.dispatch('reason_array_type', 'snowplow_identities')()) }}
{%- endmacro -%}

{% macro snowflake__reason_array_type() %}array{%- endmacro -%}

{% macro bigquery__reason_array_type() %}array<string>{%- endmacro -%}

{% macro array_concat_agg(col) %}
  {{ return(adapter.dispatch('array_concat_agg', 'snowplow_identities')(col)) }}
{%- endmacro -%}

{% macro snowflake__array_concat_agg(col) %}
  array_flatten(array_agg(coalesce({{ col }}, array_construct())))
{%- endmacro -%}

{#- array_concat_agg ignores null inputs and returns null for an all-null group, which
    array_sort_distinct then turns back into an empty array. -#}
{% macro bigquery__array_concat_agg(col) %}
  array_concat_agg({{ col }})
{%- endmacro -%}

{% macro array_sort_distinct(col) %}
  {{ return(adapter.dispatch('array_sort_distinct', 'snowplow_identities')(col)) }}
{%- endmacro -%}

{% macro snowflake__array_sort_distinct(col) %}
  array_sort(array_distinct(coalesce({{ col }}, array_construct())))
{%- endmacro -%}

{#- unnest of a null array yields no rows, so a null input becomes an empty array. -#}
{% macro bigquery__array_sort_distinct(col) %}
  array(select distinct x from unnest({{ col }}) as x order by x)
{%- endmacro -%}
