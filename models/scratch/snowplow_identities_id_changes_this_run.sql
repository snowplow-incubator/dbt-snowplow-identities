{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

with unnesting as (
 {{ extract_merged() }}
),

id_changes as (
    -- New identity creations
    select
        snowplow_id,
        cast(null as string) as previous_snowplow_id,
        first_derived_tstamp as effective_at,
        'created' as change_type,
        first_seen_event_id,
        first_app_id as first_seen_app_id

    from {{ ref('snowplow_identities_new_identities_this_run') }} e

    union all

    -- Merge events
        select
            m.active_snowplow_id as snowplow_id,
            m.snowplow_id as previous_snowplow_id,
            m.merged_at as effective_at,
            'merged' as change_type,
            m.triggering_event_id as first_seen_event_id,
            coalesce(n.first_app_id, h.first_app_id) as first_seen_app_id
        from unnesting m
        left join {{ ref('snowplow_identities_new_identities_this_run') }} n
          on m.snowplow_id = n.snowplow_id
        left join {{ ref('snowplow_identities_new_identities') }} h
          on m.snowplow_id = h.snowplow_id
)

select
    {{ dbt_utils.generate_surrogate_key(['snowplow_id', 'previous_snowplow_id', 'effective_at']) }} as id_change_key,
    snowplow_id,
    previous_snowplow_id,
    effective_at,
    {{ snowplow_utils.current_timestamp_in_utc() }} as changed_at,
    change_type,
    first_seen_event_id,
    first_seen_app_id
from id_changes
