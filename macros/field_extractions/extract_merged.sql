{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro extract_merged() %}
  {{ return(adapter.dispatch('extract_merged', 'snowplow_identities')()) }}
{%- endmacro -%}

{% macro snowflake__extract_merged() %}
    select
        p.active_snowplow_id,
        m.value:snowplowId::STRING AS snowplow_id,
        m.value:mergedAt::timestamp AS merged_at,
        min(m.value:triggeringEventId::STRING) AS triggering_event_id
    from {{ ref('snowplow_identities_merge_events_this_run') }} as p,
        table(flatten(input => p.merged)) as m
    group by 1, 2, 3
{% endmacro %}

{% macro bigquery__extract_merged() %}
    select
        p.active_snowplow_id,
        m.snowplow_id as snowplow_id,
        m.merged_at as merged_at,
        min(m.triggering_event_id) as triggering_event_id
    from {{ ref('snowplow_identities_merge_events_this_run') }} as p,
        unnest(p.merged) as m
    group by 1, 2, 3
{% endmacro %}
