{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{#
  Membership test over the string arrays the identity context carries, decision_reasons
  being the only one today. Every dialect below returns null for a null array, so each one
  coalesces: a reason the identifier does not carry and a reason nothing was recorded for
  are the same answer to a caller asking whether it applies.
#}

{% macro reason_contains(col, value) %}
  {{ return(adapter.dispatch('reason_contains', 'snowplow_identities')(col, value)) }}
{%- endmacro -%}

{% macro snowflake__reason_contains(col, value) %}
  coalesce(array_contains('{{ value }}'::variant, {{ col }}), false)
{%- endmacro -%}

{% macro bigquery__reason_contains(col, value) %}
  coalesce('{{ value }}' in unnest({{ col }}), false)
{%- endmacro -%}
