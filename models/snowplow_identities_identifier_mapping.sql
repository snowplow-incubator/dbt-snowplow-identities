{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{#
  External identifiers linked to their current active_snowplow_id. Resolves
  identifier_mapping_base against snowplow_id_mapping at read time, so a merge never
  rewrites a stored row. One row per (active_snowplow_id, id_type, id_value).
#}

{{ config(
    materialized="view",
    tags=["derived"]
) }}

with resolved as (
    select
        coalesce(m.active_snowplow_id, b.snowplow_id) as active_snowplow_id,
        b.id_type,
        b.id_value,
        b.first_app_id,
        b.last_app_id,
        b.first_seen_at,
        b.last_seen_at,
        b.first_seen_event_id,
        b.decision_reasons
    from {{ ref('snowplow_identities_identifier_mapping_base') }} b
    left join {{ ref('snowplow_identities_snowplow_id_mapping') }} m
        on b.snowplow_id = m.snowplow_id
)

-- Several snowplow_ids can carry the same identifier and resolve to one parent, so
-- collapse to one row spanning them all.
, ranked as (
    select
        active_snowplow_id,
        id_type,
        id_value,
        first_value(first_app_id) over (partition by active_snowplow_id, id_type, id_value order by first_seen_at asc, first_seen_event_id asc) as first_app_id,
        first_value(last_app_id) over (partition by active_snowplow_id, id_type, id_value order by last_seen_at desc, last_app_id desc, first_seen_event_id asc) as last_app_id,
        min(first_seen_at) over (partition by active_snowplow_id, id_type, id_value) as first_seen_at,
        max(last_seen_at) over (partition by active_snowplow_id, id_type, id_value) as last_seen_at,
        first_value(first_seen_event_id) over (partition by active_snowplow_id, id_type, id_value order by first_seen_at asc, first_seen_event_id asc) as first_seen_event_id,
        -- A reason is recorded against the snowplow_id the service decided on, and several
        -- of those can resolve to one parent, so a flag has to span the group rather than
        -- describe whichever row sorts first. Reduced to 0/1 here rather than carrying the
        -- array through: no supported warehouse aggregates arrays inside a window.
        max(case when {{ snowplow_identities.reason_contains('decision_reasons', 'merge_limit_exceeded') }} then 1 else 0 end) over (partition by active_snowplow_id, id_type, id_value) as owner_merge_limited,
        max(case when {{ snowplow_identities.reason_contains('decision_reasons', 'unique_identifier_conflict') }} then 1 else 0 end) over (partition by active_snowplow_id, id_type, id_value) as owner_unique_conflict,
        row_number() over (partition by active_snowplow_id, id_type, id_value order by first_seen_at asc, first_seen_event_id asc) as rn
    from resolved
)
, deduped as (
    select
        active_snowplow_id,
        id_type,
        id_value,
        first_app_id,
        last_app_id,
        first_seen_at,
        last_seen_at,
        first_seen_event_id,
        owner_merge_limited,
        owner_unique_conflict
    from ranked
    where rn = 1
)

, identity_ages as (
    select snowplow_id, min(created_at) as created_at
    from {{ ref('snowplow_identities_identities') }}
    group by 1
)

-- An identifier can sit under several identities at once. The engine does that when it
-- refuses a merge and links instead, and it also happens when an identifier reappears
-- after its state has expired. Those two are indistinguishable from here unless the engine
-- said which it was, so report how many owners an identifier has and prefer the one holding
-- its most recent sighting. rank rather than row_number, so owners tied on both timestamps
-- stay tied and no arbitrary winner is invented.
, ranked_owners as (
    select
        d.*,
        count(*) over (partition by d.id_type, d.id_value) as identity_count,
        sum(case when a.created_at is null then 1 else 0 end) over (partition by d.id_type, d.id_value) as undated_count,
        rank() over (partition by d.id_type, d.id_value order by d.last_seen_at desc, a.created_at asc nulls last) as pick_rank
    from deduped d
    left join identity_ages a
        on d.active_snowplow_id = a.snowplow_id
)

-- One owner carrying merge_limit_exceeded is the engine naming the identity it linked to,
-- so it answers what pick_rank can only guess at. Several carrying it name no one owner,
-- and a unique identifier conflict describes a situation where collapsing an identifier
-- onto one owner is unsafe whatever else was reported, so both counts have to be known
-- before either can be acted on.
, stated as (
    select
        *,
        sum(owner_merge_limited) over (partition by id_type, id_value) as merge_limited_owners,
        sum(owner_unique_conflict) over (partition by id_type, id_value) as unique_conflict_owners,
        sum(case when pick_rank = 1 then 1 else 0 end) over (partition by id_type, id_value) as tied_count
    from ranked_owners
)

select
    {{ dbt_utils.generate_surrogate_key(['active_snowplow_id', 'id_type', 'id_value']) }} as uuid,
    active_snowplow_id,
    id_type,
    id_value,
    first_app_id,
    last_app_id,
    first_seen_at,
    last_seen_at,
    first_seen_event_id,
    -- merge_limited is tested before unranked because the reason resolves the ambiguity
    -- unranked exists to report: once the engine has named the owner there is nothing left
    -- for the timestamp tie-break to decide.
    case
        when identity_count = 1 then 'single'
        when merge_limited_owners = 1 and unique_conflict_owners = 0 then 'merge_limited'
        when undated_count > 0 or tied_count > 1 then 'unranked'
        else 'multiple'
    end as mapping_state,
    case
        when identity_count = 1 then true
        when merge_limited_owners = 1 and unique_conflict_owners = 0 then owner_merge_limited = 1
        {%- if var('snowplow__merge_limit_collapse', false) %}
        when undated_count > 0 or tied_count > 1 then false
        else pick_rank = 1
        {%- else %}
        else false
        {%- endif %}
    end as is_preferred
from stated
