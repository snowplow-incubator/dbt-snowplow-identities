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
    },
    meta={'upsert_date_key': 'load_tstamp', 'snowplow_optimize': true}
) }}

with merge_source as (
    select *
    from {{ ref('snowplow_identities_merge_events') }} e
    where {{ snowplow_identities.get_incremental_filter('e') }}
),

all_direct_mappings as (
    -- child -> immediate parent edges from this window
    {{ snowplow_identities.extract_merged('merge_source') }}
),

true_parents as (
    -- active ids never themselves a child: the current roots
    select distinct active_snowplow_id
    from all_direct_mappings
    where active_snowplow_id not in (select snowplow_id from all_direct_mappings)
),

deduplicated as (
    -- keep edges under a current root, then the most recent mapping per child
    select
        n.snowplow_id,
        n.active_snowplow_id,
        n.merged_at,
        n.merge_event_id,
        n.triggering_event_id,
        n.load_tstamp
    from all_direct_mappings n
    inner join true_parents t
        on t.active_snowplow_id = n.active_snowplow_id
    qualify row_number() over (partition by n.snowplow_id order by n.merged_at desc) = 1
)

select
    snowplow_id,
    active_snowplow_id,
    merged_at,
    merge_event_id,
    triggering_event_id,
    load_tstamp,
    {{ snowplow_utils.current_timestamp_in_utc() }} as model_tstamp
from deduplicated
