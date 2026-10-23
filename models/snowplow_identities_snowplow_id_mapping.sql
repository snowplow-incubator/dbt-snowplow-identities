{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{#
  Current child -> active-parent mapping. Join your events' snowplow_id here to resolve
  them to a single unified identity.

  Kept flat: an active_snowplow_id never also appears as a snowplow_id, which is what lets
  identifier_mapping resolve in one hop. Asserted by tests/snowplow_id_mapping_no_chains.sql.
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
    cluster_by=snowplow_identities.get_cluster_by_values('snowplow_id_mapping'),
    tags=["derived"],
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    }
) }}

with merge_source as (
    select *
    from {{ ref('snowplow_identities_merge_events') }} e
    where {{ snowplow_identities.get_incremental_filter('e', anchor_model='snowplow_identities_merge_events') }}
),

new_mappings as (
    {{ snowplow_identities.extract_merged('merge_source') }}
),

all_direct_mappings as (
    -- A delayed event may name a parent that has already merged. Resolve it against
    -- the stored flat mapping before combining this batch with the existing edges.
    select
        n.snowplow_id,
        {% if is_incremental() %}
        coalesce(p.active_snowplow_id, n.active_snowplow_id) as active_snowplow_id,
        {% else %}
        n.active_snowplow_id,
        {% endif %}
        n.merged_at,
        n.merge_event_id,
        n.triggering_event_id,
        n.load_tstamp
    from new_mappings n
    {% if is_incremental() %}
    left join {{ this }} p on n.active_snowplow_id = p.snowplow_id

    union all

    select snowplow_id, active_snowplow_id, merged_at, merge_event_id,
           triggering_event_id, load_tstamp
    from {{ this }}
    {% endif %}
),

true_parents as (
    select distinct active_snowplow_id
    from all_direct_mappings
    where active_snowplow_id not in (select snowplow_id from all_direct_mappings)
),

root_mappings as (
    -- Cumulative merge arrays include the direct edges to the surviving root.
    select n.*
    from all_direct_mappings n
    inner join true_parents t on t.active_snowplow_id = n.active_snowplow_id
    qualify row_number() over (
        partition by n.snowplow_id
        order by n.merged_at desc, n.load_tstamp desc, n.merge_event_id asc
    ) = 1
),

resolved as (
    -- Also move stored children when their old root merges in this batch. They
    -- may have expired out of the service's cumulative array in the meantime.
    select
        n.snowplow_id,
        coalesce(p.active_snowplow_id, n.active_snowplow_id) as active_snowplow_id,
        n.merged_at,
        n.merge_event_id,
        n.triggering_event_id,
        greatest(n.load_tstamp, coalesce(p.load_tstamp, n.load_tstamp)) as load_tstamp
    from all_direct_mappings n
    left join root_mappings p on n.active_snowplow_id = p.snowplow_id
    where p.snowplow_id is not null
       or n.active_snowplow_id in (select active_snowplow_id from true_parents)
),

deduplicated as (
    select *
    from resolved
    qualify row_number() over (
        partition by snowplow_id
        order by merged_at desc, load_tstamp desc, merge_event_id asc
    ) = 1
)

select
    n.snowplow_id,
    n.active_snowplow_id,
    n.merged_at,
    n.merge_event_id,
    n.triggering_event_id,
    n.load_tstamp,
    {{ snowplow_utils.current_timestamp_in_utc() }} as model_tstamp
    {{ snowplow_identities.databricks_partition_date('n.load_tstamp', 'load_tstamp') }}
from deduplicated n
{% if is_incremental() %}
left join {{ this }} t on n.snowplow_id = t.snowplow_id
-- Retain untouched rows without rewriting the entire mapping on each run.
where t.snowplow_id is null
   or n.active_snowplow_id != t.active_snowplow_id
   or n.merged_at != t.merged_at
   or n.load_tstamp > t.load_tstamp
{% endif %}
