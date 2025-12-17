{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

with id_changes as (
    select
        snowplow_id,
        null as previous_snowplow_id,
        created_at as changed_at,
        first_derived_tstamp as triggering_event_timestamp,
        'created' as change_type,
        triggering_event_id,
        first_app_id as triggering_app_id
        
    from {{ ref('snowplow_identities_new_identities_this_run') }} e

    union all

    select
        p.active_snowplow_id as snowplow_id,
        m.snowplow_id as previous_snowplow_id,
        m.merged_at as changed_at,
        NULL as triggering_event_timestamp,
        'merged' as change_type,
        m.triggering_event_id,
        n.first_app_id as triggering_app_id
        
    from {{ ref('snowplow_identities_merge_events_this_run') }} as p,
    UNNEST(p.merged) AS m
    left join {{ ref('snowplow_identities_new_identities_this_run') }} n
    on m.snowplow_id = n.snowplow_id
)

select
    {{ dbt_utils.generate_surrogate_key(['snowplow_id', 'previous_snowplow_id']) }} as id_change_key,
    snowplow_id,
    previous_snowplow_id,
    changed_at,
    triggering_event_timestamp,
    change_type,
    triggering_event_id,
    triggering_app_id
from id_changes
