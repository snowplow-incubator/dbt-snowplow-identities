{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{#
  One row per snowplow_id: when the identity was created, and where it was first and last
  seen. snowplow_id is the identity at event time and may since have been merged.
  Per-identifier values live in identifier_mapping.
#}

{{ config(
    materialized="incremental",
    on_schema_change="append_new_columns",
    unique_key="snowplow_id",
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    partition_by=snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "load_tstamp",
      "data_type": "timestamp"
    }, databricks_val='load_tstamp_date'),
    cluster_by=snowplow_identities.get_cluster_by_values('identities'),
    tags=["derived"],
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    meta={'upsert_date_key': 'load_tstamp', 'snowplow_optimize': true}
) }}

with new_events as (
    select *
    from {{ ref('snowplow_identities_stg_identity_events') }} s
    where {{ snowplow_identities.get_incremental_filter('s') }}
),

first_event as (
    select *
    from new_events
    qualify row_number() over (
        partition by snowplow_id
        order by derived_tstamp asc, event_id asc
    ) = 1
),

last_event as (
    select *
    from new_events
    qualify row_number() over (
        partition by snowplow_id
        order by derived_tstamp desc, event_id desc
    ) = 1
),

-- Newest load_tstamp for the identity, not the first/last event's: the watermark advances
-- off this.
load_wm as (
    select snowplow_id, max(load_tstamp) as load_tstamp
    from new_events
    group by 1
),

this_run as (
    select
        f.snowplow_id,
        f.created_at,
        f.event_id as first_seen_event_id,
        f.app_id as first_app_id,
        l.app_id as last_app_id,
        f.derived_tstamp as first_derived_tstamp,
        l.derived_tstamp as last_derived_tstamp,
        w.load_tstamp
    from first_event f
    left join last_event l using (snowplow_id)
    left join load_wm w using (snowplow_id)
)

{% if is_incremental() %}

, merged as (
    select
        n.*,
        t.first_derived_tstamp < n.first_derived_tstamp as old_is_earlier,
        t.last_derived_tstamp > n.last_derived_tstamp as old_is_later,
        t.created_at as prev_created_at,
        t.first_seen_event_id as prev_first_seen_event_id,
        t.first_app_id as prev_first_app_id,
        t.last_app_id as prev_last_app_id,
        t.first_derived_tstamp as prev_first_derived_tstamp,
        t.last_derived_tstamp as prev_last_derived_tstamp,
        t.load_tstamp as prev_load_tstamp
    from this_run n
    left join {{ this }} t using (snowplow_id)
)

select
    snowplow_id,
    case when old_is_earlier then prev_created_at else created_at end as created_at,
    case when old_is_earlier then prev_first_seen_event_id else first_seen_event_id end as first_seen_event_id,
    case when old_is_earlier then prev_first_app_id else first_app_id end as first_app_id,
    case when old_is_later then prev_last_app_id else last_app_id end as last_app_id,
    case when old_is_earlier then prev_first_derived_tstamp else first_derived_tstamp end as first_derived_tstamp,
    case when old_is_later then prev_last_derived_tstamp else last_derived_tstamp end as last_derived_tstamp,
    greatest(load_tstamp, coalesce(prev_load_tstamp, load_tstamp)) as load_tstamp
from merged

{% else %}

select *
from this_run

{% endif %}
