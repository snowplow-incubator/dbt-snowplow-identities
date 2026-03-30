{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

WITH unnesting as (
    {{ extract_merged() }}
),

all_direct_mappings AS (
    -- Direct child -> parent relationships, already deduplicated by extract_merged
    SELECT
        active_snowplow_id,
        snowplow_id,
        merged_at
    FROM unnesting
),

true_parents AS (
    -- Find active IDs that were never themselves merged away
    SELECT DISTINCT active_snowplow_id
    FROM all_direct_mappings
    WHERE active_snowplow_id NOT IN (SELECT snowplow_id FROM all_direct_mappings)
),

deduplicated AS (
    -- Keep only mappings where the active ID is a true parent
    SELECT n.*
    FROM all_direct_mappings n
    INNER JOIN true_parents t
    ON t.active_snowplow_id = n.active_snowplow_id
),

ranked AS (
    -- One row per snowplow_id: keep the most recent mapping
    SELECT
        snowplow_id,
        active_snowplow_id,
        merged_at,
        ROW_NUMBER() OVER (PARTITION BY snowplow_id ORDER BY merged_at DESC) AS rn
    FROM deduplicated
)

SELECT
    snowplow_id,
    active_snowplow_id,
    merged_at,
    {{ snowplow_utils.current_timestamp_in_utc() }} as model_tstamp
FROM ranked
WHERE rn = 1
