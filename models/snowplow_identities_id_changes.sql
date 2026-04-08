{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{ config(
    materialized="incremental", 
    on_schema_change="append_new_columns", 
    unique_key="id_change_key", 
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')), 
    partition_by=snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "effective_at",
      "data_type": "timestamp"
    }, databricks_val='effective_at_date'), 
    cluster_by=snowplow_identities.get_cluster_by_values('id_changes'), 
    tags=["derived"], 
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    }, 
    meta={'upsert_date_key': 'effective_at', 'snowplow_optimize': true}
) }}

{% if not is_incremental() %}

  select *
  from {{ ref('snowplow_identities_id_changes_this_run') }}
  where {{ snowplow_utils.is_run_with_new_events('snowplow_identities') }}
  
{% else %}

select 

    i.id_change_key,
    i.snowplow_id,
    i.previous_snowplow_id,
    -- Always keep the earliest effective_at and changed at for any late arriving data
    least(i.effective_at, coalesce(t.effective_at, i.effective_at)) as effective_at,
    least(i.changed_at, coalesce(t.changed_at, i.changed_at)) as changed_at,
    i.change_type,
    case when t.effective_at is null or i.effective_at < t.effective_at then i.first_seen_event_id
      else t.first_seen_event_id end as first_seen_event_id,
    case when t.effective_at is null or i.effective_at < t.effective_at then i.first_seen_app_id
      else t.first_seen_app_id end as first_seen_app_id

from {{ ref('snowplow_identities_id_changes_this_run') }} i
left join {{ this }} t
on i.id_change_key = t.id_change_key
where {{ snowplow_utils.is_run_with_new_events('snowplow_identities') }}

{% endif %}
