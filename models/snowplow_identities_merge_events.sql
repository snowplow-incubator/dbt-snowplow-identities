{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{#
  Append-only log of identity_merge events, read straight from atomic.events and
  self-watermarked off its own max(load_tstamp). The lossless record of every merge, and
  what snowplow_id_mapping resolves current state from.

  Keep this table: it is the only place the raw merged/merges payloads are retained, so it
  is what any audit or change-log rebuild must be based on (see MIGRATION.md).
#}

{{ config(
    materialized="incremental",
    on_schema_change='append_new_columns',
    unique_key='merge_event_id',
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "load_tstamp",
      "data_type": "timestamp"
    }, databricks_val='load_tstamp_date'),
    cluster_by=snowplow_identities.get_cluster_by_values('merge_events'),
    tags=["derived"],
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    meta={'upsert_date_key': 'load_tstamp', 'snowplow_optimize': true}
) }}

with prep as (
    select
      event_id as merge_event_id
      {{ snowplow_identities.get_merge_fields() }},
      collector_tstamp,
      derived_tstamp,
      load_tstamp
    from {{ source('atomic', 'events') }}
    where event_name = 'identity_merge'
    and {{ snowplow_identities.get_incremental_filter(use_atomic_partition=true) }}
    {%- if var('snowplow__app_id', []) | length > 0 %}
    and app_id in ({% for aid in var('snowplow__app_id', []) %}'{{ aid }}'{% if not loop.last %}, {% endif %}{% endfor %})
    {%- endif %}
    qualify row_number() over (partition by event_id order by collector_tstamp) = 1
)

select
    merge_event_id,
    active_snowplow_id,
    collector_tstamp,
    derived_tstamp,
    merged,
    merges,
    load_tstamp
from prep
