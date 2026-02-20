{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro get_merge_fields() %}
  {{ return(adapter.dispatch('get_merge_fields', 'snowplow_identities')()) }}
{%- endmacro -%}

{% macro bigquery__get_merge_fields() %}

  {% set bq_identity_fields = [
    {'field':('snowplow_id', 'active_snowplow_id'), 'dtype':'string'},
    {'field':('merged', 'merged'), 'dtype':'string'},
    {'field':('merges', 'merges'), 'dtype':'array<string>'}
    ] %}

  ,  {{ snowplow_utils.get_optional_fields(
        enabled=true,
        col_prefix='unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1',
        fields=bq_identity_fields,
        relation=source('atomic', 'events') if 'integration_tests' in project_name and 'snowplow' in project_name else source('atomic', 'events') ,
        relation_alias=none) }}

{% endmacro %}

{% macro snowflake__get_merge_fields() %}

    , unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1:snowplowId::varchar AS active_snowplow_id
    , unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1:merged::array AS merged
    , unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1:merges::array AS merges

{% endmacro %}
