{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{#
  Flatten the `merged` array of a merge-events relation into one row per (parent, child)
  edge. `source_relation` is the name of a CTE or relation carrying active_snowplow_id,
  merge_event_id, merged and load_tstamp.

  triggering_event_id comes from the array element and is stable across the cumulative
  re-reports of an edge, so it is a reliable audit pointer. merge_event_id is the
  identity_merge event that carried the edge; because the arrays are cumulative, several
  merge events re-list the same edge, so this is min() over those seen in the current
  window and can differ between runs.
#}

{% macro extract_merged(source_relation) %}
  {{ return(adapter.dispatch('extract_merged', 'snowplow_identities')(source_relation)) }}
{%- endmacro -%}

{% macro default__extract_merged(source_relation) %}
  {{ exceptions.raise_compiler_error(
    "The `extract_merged` macro is not implemented for adapter `"
    ~ target.type
    ~ "`. Supported adapters are: `snowflake`, `bigquery`."
  ) }}
{% endmacro %}

{% macro snowflake__extract_merged(source_relation) %}
    select
        p.active_snowplow_id,
        m.value:snowplow_id::string as snowplow_id,
        m.value:merged_at::timestamp as merged_at,
        min(p.merge_event_id) as merge_event_id,
        min(m.value:triggering_event_id::string) as triggering_event_id,
        max(p.load_tstamp) as load_tstamp
    from {{ source_relation }} as p,
        table(flatten(input => p.merged)) as m
    group by 1, 2, 3
{% endmacro %}

{% macro bigquery__extract_merged(source_relation) %}
    select
        p.active_snowplow_id,
        m.snowplow_id as snowplow_id,
        m.merged_at as merged_at,
        min(p.merge_event_id) as merge_event_id,
        min(m.triggering_event_id) as triggering_event_id,
        max(p.load_tstamp) as load_tstamp
    from {{ source_relation }} as p,
        unnest(p.merged) as m
    group by 1, 2, 3
{% endmacro %}
