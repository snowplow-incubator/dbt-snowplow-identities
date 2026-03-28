{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{ config(
    materialized="incremental", 
    on_schema_change="append_new_columns", 
    unique_key="snowplow_id", 
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')), 
    partition_by=snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "merged_at",
      "data_type": "timestamp"
    }, databricks_val='merged_at_date'), 
    cluster_by=snowplow_identities.get_cluster_by_values('snowplow_id_mapping'), 
    tags=["derived"], 
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    }, 
    meta={'upsert_date_key': 'merged_at', 'snowplow_optimize': true}
) }}

{% if is_incremental() %}

with recursive new_from_this_run as (
    select *
    from {{ ref('snowplow_identities_snowplow_id_mapping_this_run') }}
    where {{ snowplow_utils.is_run_with_new_events('snowplow_identities') }}
),

-- First pass: resolve each new mapping one hop into history
history_resolved as (
    select
        m.snowplow_id,
        coalesce(hist.active_snowplow_id, m.active_snowplow_id) as active_snowplow_id,
        m.merged_at,
        m.model_tstamp
    from new_from_this_run m
    left join {{ this }} hist
        on m.active_snowplow_id = hist.snowplow_id
),

-- Recursive pass: if the resolved parent is itself a new child,
-- follow that edge and do another history lookup
resolved as (
    select snowplow_id, active_snowplow_id, merged_at, model_tstamp, 1 as depth
    from history_resolved

    union all

    select r.snowplow_id,
           coalesce(hist.active_snowplow_id, n.active_snowplow_id) as active_snowplow_id,
           r.merged_at, r.model_tstamp, r.depth + 1
    from resolved r
    inner join new_from_this_run n on r.active_snowplow_id = n.snowplow_id
    left join {{ this }} hist on n.active_snowplow_id = hist.snowplow_id
    where r.depth < {{ var('snowplow__max_merge_depth') }}
),

ranked as (
    select
        snowplow_id,
        active_snowplow_id,
        merged_at,
        model_tstamp,
        row_number() over (partition by snowplow_id order by depth desc, merged_at desc) as rn
    from resolved
),

resolved_this_run as (
    select snowplow_id, active_snowplow_id, merged_at, model_tstamp
    from ranked
    where rn = 1
),

existing_to_repoint as (
    select
        hist.snowplow_id,
        m.active_snowplow_id,
        hist.merged_at,
        {{ snowplow_utils.current_timestamp_in_utc() }} as model_tstamp
    from {{ this }} hist
    inner join resolved_this_run m
        on hist.active_snowplow_id = m.snowplow_id
    where not exists (
        select 1 from resolved_this_run n where n.snowplow_id = hist.snowplow_id
    )
)

select * from resolved_this_run
union all
select * from existing_to_repoint

{% else %}

select *
from {{ ref('snowplow_identities_snowplow_id_mapping_this_run') }}
where {{ snowplow_utils.is_run_with_new_events('snowplow_identities') }}

{% endif %}
