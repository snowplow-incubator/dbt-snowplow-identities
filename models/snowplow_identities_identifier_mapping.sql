{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{ config(
    materialized="incremental",
    on_schema_change="append_new_columns",
    unique_key="uuid",
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    partition_by=snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "last_seen_at",
      "data_type": "timestamp"
    }, databricks_val='last_seen_at_date'),
    cluster_by=snowplow_identities.get_cluster_by_values('identifier_mapping'),
    tags=["derived"],
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    meta={'upsert_date_key': 'last_seen_at', 'snowplow_optimize': true}
) }}

with new_from_this_run as (
    select *
    from {{ ref('snowplow_identities_identifier_mapping_this_run') }}
    where {{ snowplow_utils.is_run_with_new_events('snowplow_identities') }}
)

, merged_ids_this_run as (
    select distinct snowplow_id
    from {{ ref('snowplow_identities_snowplow_id_mapping_this_run') }}
)

, id_mapping as (
    select snowplow_id, active_snowplow_id
    from {{ ref('snowplow_identities_snowplow_id_mapping') }}
)

{% if is_incremental() %}

, existing_to_repoint as (
    select
        hist.uuid,
        coalesce(id_map.active_snowplow_id, hist.active_snowplow_id) as active_snowplow_id,
        hist.id_type,
        hist.id_value,
        hist.first_app_id,
        hist.last_app_id,
        hist.first_seen_at,
        hist.last_seen_at,
        hist.first_seen_event_id
    from {{ this }} hist
    left join id_mapping id_map
        on hist.active_snowplow_id = id_map.snowplow_id
    where hist.active_snowplow_id in (select snowplow_id from merged_ids_this_run)
    and not exists (
        select 1 from new_from_this_run n where n.uuid = hist.uuid
    )
)

, new_with_history as (
    select
        c.uuid,
        c.active_snowplow_id,
        c.id_type,
        c.id_value,
        case when h.first_seen_at is not null and h.first_seen_at <= c.first_seen_at
             then h.first_app_id else c.first_app_id end as first_app_id,
        case when h.last_seen_at is not null and h.last_seen_at >= c.last_seen_at
             then h.last_app_id else c.last_app_id end as last_app_id,
        least(c.first_seen_at, coalesce(h.first_seen_at, c.first_seen_at)) as first_seen_at,
        greatest(c.last_seen_at, coalesce(h.last_seen_at, c.last_seen_at)) as last_seen_at,
        case when h.first_seen_at is not null and h.first_seen_at <= c.first_seen_at
             then h.first_seen_event_id else c.first_seen_event_id end as first_seen_event_id
    from new_from_this_run c
    left join {{ this }} h
        on c.uuid = h.uuid
)

, new_ranked as (
    select
        uuid,
        active_snowplow_id,
        id_type,
        id_value,
        first_value(first_app_id) over (partition by uuid, active_snowplow_id, id_type, id_value order by first_seen_at asc) as first_app_id,
        first_value(last_app_id) over (partition by uuid, active_snowplow_id, id_type, id_value order by last_seen_at desc) as last_app_id,
        min(first_seen_at) over (partition by uuid, active_snowplow_id, id_type, id_value) as first_seen_at,
        max(last_seen_at) over (partition by uuid, active_snowplow_id, id_type, id_value) as last_seen_at,
        first_value(first_seen_event_id) over (partition by uuid, active_snowplow_id, id_type, id_value order by first_seen_at asc) as first_seen_event_id,
        row_number() over (partition by uuid, active_snowplow_id, id_type, id_value order by first_seen_at asc) as rn
    from new_with_history
)

, new_aggregated as (
    select
        uuid,
        active_snowplow_id,
        id_type,
        id_value,
        first_app_id,
        last_app_id,
        first_seen_at,
        last_seen_at,
        first_seen_event_id
    from new_ranked
    where rn = 1
)

select * from new_aggregated
union all
select * from existing_to_repoint

{% else %}

select *
from new_from_this_run

{% endif %}
