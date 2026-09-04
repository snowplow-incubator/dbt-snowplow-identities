{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro get_identity_fields() %}
  {{ return(adapter.dispatch('get_identity_fields', 'snowplow_identities')()) }}
{%- endmacro -%}

{% macro bigquery__get_identity_fields() %}

  {% set bq_identity_fields = [
    {'field':('snowplow_id', 'snowplow_id'), 'dtype':'string'},
    {'field':('created_at', 'created_at'), 'dtype':'timestamp'}
    ] %}

  {{ snowplow_utils.get_optional_fields(
        enabled=true,
        col_prefix='contexts_com_snowplowanalytics_snowplow_identity_2',
        fields=bq_identity_fields,
        relation=source('atomic', 'events') if 'integration_tests' in project_name and 'snowplow' in project_name else source('atomic', 'events') ,
        relation_alias=none) }},

  {#
    decision_reasons is a repeated field, which get_optional_fields cannot express
    (it casts to a scalar dtype). The column check keeps warehouses that have never
    received an identity event compiling.
  #}
  {% set identity_col = 'contexts_com_snowplowanalytics_snowplow_identity_2' %}
  {% if identity_col in (adapter.get_columns_in_relation(source('atomic','events')) | map(attribute='name') | map('lower') | list) %}
    {{ identity_col }}[safe_offset(0)].decision_reasons as decision_reasons,
  {% else %}
    cast(null as array<string>) as decision_reasons,
  {% endif %}

{% endmacro %}

{% macro snowflake__get_identity_fields() %}

    contexts_com_snowplowanalytics_snowplow_identity_2[0]:snowplow_id::varchar as snowplow_id,
    contexts_com_snowplowanalytics_snowplow_identity_2[0]:created_at::timestamp as created_at,
    contexts_com_snowplowanalytics_snowplow_identity_2[0]:decision_reasons as decision_reasons,

{% endmacro %}
