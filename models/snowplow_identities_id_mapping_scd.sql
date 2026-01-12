{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{
  config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    unique_key=['snowplow_id', 'effective_at'],
    upsert_date_key='effective_at',
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "effective_at",
      "data_type": "timestamp",
      "granularity": "day"
    }, databricks_val='effective_at_date'),
    cluster_by=snowplow_identities.get_cluster_by_values('id_mapping_scd'),
    tags=["derived"],
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    snowplow_optimize=true
  )
}}

-- Slowly Changing Dimension (Type 2) for identity mappings
-- Uses event time (effective_at) for point-in-time reproducible queries
-- Computes validity windows from the immutable id_changes log
--
-- Point-in-time query pattern:
--   SELECT s.snowplow_id, scd.active_snowplow_id
--   FROM sessions s
--   JOIN snowplow_identities_id_mapping_scd scd ON s.snowplow_id = scd.snowplow_id
--   WHERE scd.effective_at <= @query_timestamp
--     AND (scd.superseded_at IS NULL OR scd.superseded_at > @query_timestamp)

{% set lookback_days = var('snowplow__scd_lookback_days', 1) %}

with changes_base as (
    -- Merge events: previous_snowplow_id is mapped to snowplow_id (the parent)
    select
        previous_snowplow_id as snowplow_id,
        active_snowplow_id,
        effective_at,
        changed_at,
        change_type,
        first_seen_event_id,
        first_seen_app_id
    from {{ ref('snowplow_identities_id_changes') }}
    where change_type = 'merged'
      and previous_snowplow_id is not null

    union all

    -- Create events: snowplow_id maps to itself initially
    select
        snowplow_id,
        active_snowplow_id,
        effective_at,
        changed_at,
        change_type,
        first_seen_event_id,
        first_seen_app_id
    from {{ ref('snowplow_identities_id_changes') }}
    where change_type = 'created'
),

with_superseded as (
    select
        snowplow_id,
        active_snowplow_id,
        effective_at,
        changed_at,
        change_type,
        first_seen_event_id,
        first_seen_app_id,
        -- Compute when this mapping was superseded by the next change to this snowplow_id
        lead(effective_at) over (
            partition by snowplow_id
            order by effective_at asc, first_seen_event_id asc
        ) as superseded_at
    from changes_base
)

select
    snowplow_id,
    active_snowplow_id,
    effective_at,
    superseded_at,
    changed_at,
    change_type,
    first_seen_event_id,
    first_seen_app_id,
    (superseded_at is null) as is_current

from with_superseded

{% if is_incremental() %}
-- For incremental runs, reprocess snowplow_ids that had changes since last run
where snowplow_id in (
    -- IDs that had direct changes (merges where they are the child)
    select distinct previous_snowplow_id
    from {{ ref('snowplow_identities_id_changes') }}
    where change_type = 'merged'
      and previous_snowplow_id is not null
      and changed_at >= (select max(changed_at) from {{ this }}) - interval {{ lookback_days }} day

    union distinct

    -- IDs that were created
    select distinct snowplow_id
    from {{ ref('snowplow_identities_id_changes') }}
    where change_type = 'created'
      and changed_at >= (select max(changed_at) from {{ this }}) - interval {{ lookback_days }} day
)
{% endif %}
