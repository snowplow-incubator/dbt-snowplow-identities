{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{ config(
    materialized="incremental",
    on_schema_change="append_new_columns",
    unique_key="scd_key",
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    partition_by=snowplow_utils.get_value_by_target_type(bigquery_val = {
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
    meta={'upsert_date_key': 'effective_at', 'snowplow_optimize': true}
) }}

WITH ids_affected_this_run AS (
    SELECT DISTINCT snowplow_id
    FROM {{ ref('snowplow_identities_id_changes_this_run') }}

    UNION ALL

    SELECT DISTINCT previous_snowplow_id
    FROM {{ ref('snowplow_identities_id_changes_this_run') }}
    WHERE change_type = 'merged'
)

{% if is_incremental() %}
    , historical_records AS (
        SELECT
            scd_key,
            snowplow_id,
            active_snowplow_id,
            effective_at,
            change_type
        FROM {{ this }}
        WHERE snowplow_id IN (SELECT snowplow_id FROM ids_affected_this_run)
    )
{% endif %}

, new_records AS (
    SELECT
        -- scd_key: stable key for the incremental merge.
        -- For "merged" rows: includes effective_at so different merge events for the same child
        -- get distinct keys, but is parent-agnostic (handles cumulative re-emission).
        -- For "created" rows: excludes effective_at so late-arriving events that backdate the
        -- creation timestamp overwrite the original row instead of creating a duplicate.
        {{ dbt_utils.generate_surrogate_key([
            'CASE WHEN change_type = \'merged\' THEN previous_snowplow_id ELSE snowplow_id END',
            'CASE WHEN change_type = \'merged\' THEN CAST(effective_at AS STRING) ELSE \'created\' END',
            'change_type'
        ]) }} AS scd_key,
        -- The ID we are tracking (could be a child being merged or a new ID being created)
        CASE WHEN change_type = 'merged' THEN previous_snowplow_id ELSE snowplow_id END AS snowplow_id,
        snowplow_id as active_snowplow_id,
        effective_at,
        change_type
    FROM {{ ref('snowplow_identities_id_changes_this_run') }}
)

, combined AS (
    SELECT *, 1 AS source_rank FROM new_records

    {% if is_incremental() %}
        UNION ALL
        SELECT *, 2 AS source_rank FROM historical_records
    {% endif %}
)

-- Two-stage dedup:
-- 1. By scd_key: when a child's parent changes across batches (intermediate → final),
--    both rows share the same scd_key. Keep the new batch's row (correct parent).
--    For "created" rows (same parent, same scd_key), keep the earliest effective_at.
-- 2. By (snowplow_id, active_snowplow_id, change_type): collapse any remaining duplicates
--    (e.g. re-emitted "created" rows with different effective_at that got different scd_keys
--    due to the effective_at-excluded key design). Keep earliest effective_at.
, deduped_by_key AS (
    SELECT
        scd_key,
        snowplow_id,
        active_snowplow_id,
        effective_at,
        change_type
    FROM combined
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY scd_key
        ORDER BY effective_at ASC, source_rank ASC
    ) = 1
)

, deduped AS (
    SELECT
        scd_key,
        snowplow_id,
        active_snowplow_id,
        effective_at,
        change_type
    FROM deduped_by_key
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY snowplow_id, active_snowplow_id, change_type
        ORDER BY effective_at ASC
    ) = 1
)
, final_scd AS (
    SELECT
        d.scd_key,
        d.snowplow_id,
        d.active_snowplow_id,
        d.effective_at,
        LEAD(d.effective_at) OVER (PARTITION BY d.snowplow_id  ORDER BY d.effective_at ASC,
         -- For ties: current true parent comes last to stay active
         CASE WHEN t.active_snowplow_id IS NOT NULL THEN 1 ELSE 0 END ASC) AS superseded_at,
        d.change_type
    FROM deduped d
    LEFT JOIN {{ ref('snowplow_identities_snowplow_id_mapping_this_run') }} t
        ON d.snowplow_id = t.snowplow_id
        AND d.active_snowplow_id = t.active_snowplow_id
)

SELECT
    scd_key,
    snowplow_id,
    active_snowplow_id,
    effective_at,
    superseded_at,
    change_type,
    (superseded_at IS NULL) AS is_current
FROM final_scd
