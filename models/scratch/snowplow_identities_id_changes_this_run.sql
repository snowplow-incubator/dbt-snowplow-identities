{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

with unnesting as (
 {{ extract_merged() }}
),

-- Max merged_at per resolved parent — the current event's merge time.
-- Used to cascade-promote effective_at for children whose root changed.
cascade_times as (
    select
        sm.active_snowplow_id,
        max(m.merged_at) as event_merged_at
    from unnesting m
    inner join {{ ref('snowplow_identities_snowplow_id_mapping_this_run') }} sm
        on m.snowplow_id = sm.snowplow_id
    group by 1
),

-- Children confirmed as cascade (will be re-rooted by this run). Two
-- sources: (1) same child appears under multiple active_snowplow_ids in
-- this run's unnesting (full refresh / multi-event same batch), or
-- (2) child is in the pre-run snapshot snowplow_id_mapping_affected,
-- which by construction contains only rows whose parent is becoming a
-- child itself this run.
cascade_children as (
    select snowplow_id
    from unnesting
    group by snowplow_id
    having count(distinct active_snowplow_id) > 1

    union distinct

    select snowplow_id
    from {{ source('snowplow_identities_internal', 'snowplow_id_mapping_affected') }}
),

-- Intermediate parents visible in this run's unnesting: active_snowplow_id
-- values that also appear as snowplow_id. Enables emitting A→B rows
-- when the chain is observable within this batch.
intermediate_parents as (
    select distinct active_snowplow_id as intermediate_id
    from unnesting
    where active_snowplow_id in (select snowplow_id from unnesting)
),

intermediate_event_times as (
    select
        active_snowplow_id,
        max(merged_at) as event_merged_at
    from unnesting
    group by 1
),

-- Orphan cascades: children whose pre-run parent is being re-rooted in
-- this run but whose own record is absent from the event unnesting
-- (cross-batch cascades, or engine-TTL-truncated cumulative arrays).
-- The pre-hook on snowplow_id_mapping snapshots exactly these rows into
-- snowplow_id_mapping_affected before the incremental merge overwrites
-- their parent pointers.
orphan_cascades as (
    select
        curr.active_snowplow_id as snowplow_id,
        aff.snowplow_id as previous_snowplow_id,
        curr.merged_at as effective_at
    from {{ source('snowplow_identities_internal', 'snowplow_id_mapping_affected') }} aff
    inner join {{ ref('snowplow_identities_snowplow_id_mapping_this_run') }} curr
        on aff.active_snowplow_id = curr.snowplow_id
    left join unnesting u
        on u.snowplow_id = aff.snowplow_id
    where u.snowplow_id is null
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

    -- Resolved merge rows with cascade-aware effective_at.
    -- Direct children (merged_at = event max) keep their own timestamp.
    -- Within-run cascade children (flagged in cascade_children) get the
    -- event merge time instead of their original merged_at.
        select
            sm.active_snowplow_id as snowplow_id,
            m.snowplow_id as previous_snowplow_id,
            case
                when m.merged_at < ct.event_merged_at
                    and cc.snowplow_id is not null
                    then ct.event_merged_at
                else m.merged_at
            end as effective_at,
            'merged' as change_type,
            min(m.triggering_event_id) as first_seen_event_id,
            coalesce(h.first_app_id, n.first_app_id) as first_seen_app_id
        from unnesting m
        inner join {{ ref('snowplow_identities_snowplow_id_mapping_this_run') }} sm
          on m.snowplow_id = sm.snowplow_id
        inner join cascade_times ct
          on sm.active_snowplow_id = ct.active_snowplow_id
        left join cascade_children cc
          on m.snowplow_id = cc.snowplow_id
        left join {{ ref('snowplow_identities_new_identities_this_run') }} n
          on m.snowplow_id = n.snowplow_id
        left join {{ ref('snowplow_identities_new_identities') }} h
          on m.snowplow_id = h.snowplow_id
        group by 1, 2, 3, 4, 6

    union all

    -- Intermediate merge rows: within-batch chains where an intermediate
    -- parent appears both as a merge target and a merge child in this run
    -- (single-batch / full-refresh cascades).
        select
            m.active_snowplow_id as snowplow_id,
            m.snowplow_id as previous_snowplow_id,
            case
                when m.merged_at < iet.event_merged_at
                    then iet.event_merged_at
                else m.merged_at
            end as effective_at,
            'merged' as change_type,
            min(m.triggering_event_id) as first_seen_event_id,
            coalesce(h.first_app_id, n.first_app_id) as first_seen_app_id
        from unnesting m
        inner join intermediate_parents ip
          on m.active_snowplow_id = ip.intermediate_id
        inner join intermediate_event_times iet
          on m.active_snowplow_id = iet.active_snowplow_id
        left join {{ ref('snowplow_identities_new_identities_this_run') }} n
          on m.snowplow_id = n.snowplow_id
        left join {{ ref('snowplow_identities_new_identities') }} h
          on m.snowplow_id = h.snowplow_id
        group by 1, 2, 3, 4, 6

    union all

    -- Orphan cascade rows: children not in this event's unnesting whose
    -- pre-run parent is being re-rooted. first_seen_event_id falls back
    -- to the current event's triggering id since the orphan has none of
    -- its own in this batch.
        select
            oc.snowplow_id,
            oc.previous_snowplow_id,
            oc.effective_at,
            'merged' as change_type,
            min(m.triggering_event_id) as first_seen_event_id,
            coalesce(h.first_app_id, n.first_app_id) as first_seen_app_id
        from orphan_cascades oc
        inner join unnesting m
          on m.active_snowplow_id = oc.snowplow_id
         and m.merged_at = oc.effective_at
        left join {{ ref('snowplow_identities_new_identities_this_run') }} n
          on oc.previous_snowplow_id = n.snowplow_id
        left join {{ ref('snowplow_identities_new_identities') }} h
          on oc.previous_snowplow_id = h.snowplow_id
        group by 1, 2, 3, 4, 6
)

select
    {{ dbt_utils.generate_surrogate_key(['snowplow_id', 'previous_snowplow_id']) }} as id_change_key,
    snowplow_id,
    previous_snowplow_id,
    effective_at,
    {{ snowplow_utils.current_timestamp_in_utc() }} as changed_at,
    change_type,
    first_seen_event_id,
    first_seen_app_id
from id_changes
