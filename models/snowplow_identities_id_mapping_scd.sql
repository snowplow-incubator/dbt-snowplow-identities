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
    unique_key='id_change_key',
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

WITH ids_affected_this_run AS (
    SELECT DISTINCT snowplow_id 
    FROM {{ ref('snowplow_identities_id_changes_this_run') }}
)

{% if is_incremental() %}
    , historical_records AS (
        SELECT 
            id_change_key, 
            snowplow_id,
            active_snowplow_id,
            effective_at,
            change_type
        FROM {{ this }}
        WHERE snowplow_id IN (SELECT snowplow_id FROM ids_affected_this_run)
        AND superseded_at IS NULL
    )
    {% endif %}

, new_records AS (
    SELECT 
        id_change_key,
        -- The ID we are tracking (could be a child being merged or a new ID being created)
        CASE WHEN change_type = 'merged' THEN previous_snowplow_id ELSE snowplow_id END AS snowplow_id,
        active_snowplow_id,
        effective_at,
        change_type
    FROM {{ ref('snowplow_identities_id_changes_this_run') }}
)

, combined AS (
    SELECT * FROM new_records
    
    {% if is_incremental() %}
        UNION ALL
        SELECT * FROM historical_records
    {% endif %}
)

, final_scd AS (
    SELECT
        id_change_key,
        snowplow_id,
        active_snowplow_id,
        effective_at,
        LEAD(effective_at) OVER (PARTITION BY snowplow_id ORDER BY effective_at ASC) as superseded_at,
        change_type
    FROM combined
)

SELECT 
    id_change_key,
    snowplow_id,
    active_snowplow_id,
    effective_at,
    superseded_at,
    change_type,
    (superseded_at IS NULL) AS is_current
FROM final_scd
