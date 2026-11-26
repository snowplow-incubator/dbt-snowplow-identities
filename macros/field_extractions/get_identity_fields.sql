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
    (it casts to a scalar dtype). BigQuery rejects a struct field the column does not
    declare, so read it only when the context column carries it: a warehouse whose
    identity events predate the field, or that has none, stays compiling.
  #}
  {% set identity_col = 'contexts_com_snowplowanalytics_snowplow_identity_2' %}
  {% set identity_fields = [] %}
  {% for column in adapter.get_columns_in_relation(source('atomic','events')) if column.name | lower == identity_col %}
    {% do identity_fields.extend(column.fields | map(attribute='name') | map('lower') | list) %}
  {% endfor %}
  {% if 'decision_reasons' in identity_fields %}
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

{% macro spark__get_identity_fields() %}

    cast(get(contexts_com_snowplowanalytics_snowplow_identity_2, 0).snowplow_id as string) as snowplow_id,
    cast(get(contexts_com_snowplowanalytics_snowplow_identity_2, 0).created_at as timestamp) as created_at,

  {#
    A typed context struct only has decision_reasons once the loader has seen a schema
    carrying it, so read the field only when the column's type declares it.
  #}
  {%- set events = source('atomic', 'events') -%}
  {%- set identity_types = [] -%}
  {%- if events is not string -%}
    {%- set identity_types = adapter.get_columns_in_relation(events)
          | selectattr('name', 'equalto', 'contexts_com_snowplowanalytics_snowplow_identity_2')
          | map(attribute='dtype') | map('lower') | list -%}
  {%- endif -%}
  {%- if identity_types and 'decision_reasons' in identity_types[0] %}
    get(contexts_com_snowplowanalytics_snowplow_identity_2, 0).decision_reasons as decision_reasons,
  {%- else %}
    cast(null as array<string>) as decision_reasons,
  {%- endif %}

{% endmacro %}
