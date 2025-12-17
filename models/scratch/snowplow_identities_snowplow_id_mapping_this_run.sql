{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

WITH all_direct_mappings AS (
    -- Unnest the merged array to get all direct child -> parent relationships
    SELECT
        p.active_snowplow_id,
        m.snowplow_id AS snowplow_id,
        m.merged_at
    FROM {{ ref('snowplow_identities_merge_events_this_run') }} AS p,
    UNNEST(p.merged) AS m
),

true_parents AS (
    -- Find active IDs that were never themselves merged away
    SELECT DISTINCT p.active_snowplow_id
    FROM {{ ref('snowplow_identities_merge_events_this_run') }} AS p
    LEFT JOIN all_direct_mappings n 
    ON n.snowplow_id = p.active_snowplow_id
    WHERE n.snowplow_id IS NULL
),

deduplicated AS (
    -- Keep only mappings where the active ID is a true parent
    SELECT n.*
    FROM all_direct_mappings n
    INNER JOIN true_parents t
    ON t.active_snowplow_id = n.active_snowplow_id
)

SELECT 
    d.snowplow_id,
    d.active_snowplow_id,
    d.merged_at AS updated_at,
    {{ snowplow_utils.current_timestamp_in_utc() }} as model_tstamp
FROM deduplicated d
