{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

WITH RECURSIVE unnesting as (
    {{ extract_merged() }}
),

all_direct_mappings AS (
    SELECT active_snowplow_id, snowplow_id, merged_at
    FROM unnesting
),

resolved AS (
    -- Anchor: start with every direct mapping
    SELECT snowplow_id, active_snowplow_id, merged_at, 1 AS depth
    FROM all_direct_mappings

    UNION ALL

    -- Recursive step: if active_snowplow_id is itself a child, follow one hop
    SELECT r.snowplow_id, a.active_snowplow_id, r.merged_at, r.depth + 1
    FROM resolved r
    INNER JOIN all_direct_mappings a ON r.active_snowplow_id = a.snowplow_id
    WHERE r.depth < {{ var('snowplow__max_merge_depth') }}
),

ranked AS (
    -- One row per snowplow_id: keep the deepest resolution (true parent),
    -- breaking ties by most recent merged_at
    SELECT
        snowplow_id,
        active_snowplow_id,
        merged_at,
        ROW_NUMBER() OVER (PARTITION BY snowplow_id ORDER BY depth DESC, merged_at DESC) AS rn
    FROM resolved
)

SELECT
    snowplow_id,
    active_snowplow_id,
    merged_at,
    {{ snowplow_utils.current_timestamp_in_utc() }} as model_tstamp
FROM ranked
WHERE rn = 1
