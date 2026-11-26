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
        b.decision_reasons,
        b.has_healthy_sighting
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
        -- null health is a pre-upgrade row whose sightings were never classified, and unknown
        -- must count as possibly healthy or the upgrade would strip eligibility from rows
        -- that never changed.
        max(case when coalesce(has_healthy_sighting, true) then 1 else 0 end) over (partition by active_snowplow_id, id_type, id_value) as owner_maybe_healthy,
        row_number() over (partition by active_snowplow_id, id_type, id_value order by first_seen_at asc, first_seen_event_id asc) as rn
    from resolved
)
-- An owner tied to an identifier only through degraded sightings, with no merge_limit
-- label of its own, may not hold preference for it; it still counts as an owner for
-- identity_count and mapping_state. A degraded owner the service named keeps the label's
-- authority: the refusal record does not degrade with the sighting.
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
        owner_unique_conflict,
        case when owner_maybe_healthy = 0 and owner_merge_limited = 0 then 0 else 1 end as owner_eligible
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
        a.created_at as owner_created_at,
        count(*) over (partition by d.id_type, d.id_value) as identity_count,
        sum(case when a.created_at is null then 1 else 0 end) over (partition by d.id_type, d.id_value) as undated_count,
        rank() over (partition by d.id_type, d.id_value order by d.last_seen_at desc, a.created_at asc nulls last) as pick_rank,
        -- pick_rank stays the reported ranking; eligibility only decides who may hold the
        -- preference, so it is a second rank with eligibility ahead of the same ordering
        -- rather than an edit to the first.
        rank() over (partition by d.id_type, d.id_value order by d.owner_eligible desc, d.last_seen_at desc, a.created_at asc nulls last) as eligible_rank
    from deduped d
    left join identity_ages a
        on d.active_snowplow_id = a.snowplow_id
)

-- One owner carrying merge_limit_exceeded is the engine naming the identity it linked to,
-- so it answers what pick_rank can only guess at. Several carrying it name no one owner
-- directly, and a unique identifier conflict describes a situation where collapsing an
-- identifier onto one owner is unsafe whatever else was reported, so both counts have to
-- be known before either can be acted on.
, stated as (
    select
        *,
        sum(owner_merge_limited) over (partition by id_type, id_value) as merge_limited_owners,
        sum(owner_unique_conflict) over (partition by id_type, id_value) as unique_conflict_owners,
        sum(case when pick_rank = 1 then 1 else 0 end) over (partition by id_type, id_value) as tied_count,
        sum(case when eligible_rank = 1 then 1 else 0 end) over (partition by id_type, id_value) as eligible_tied_count,
        max(last_seen_at) over (partition by id_type, id_value) as latest_seen_at,
        min(case when owner_merge_limited = 1 then owner_created_at end) over (partition by id_type, id_value) as oldest_labelled_created_at,
        sum(case when owner_merge_limited = 1 and owner_created_at is null then 1 else 0 end) over (partition by id_type, id_value) as undated_labelled_count
    from ranked_owners
)

-- Several owners carrying the label mean the identifier was refused a merge more than
-- once, and the service always links into the oldest identity, so the oldest labelled
-- owner is where the identifier lives now -- unless what actually happened is a TTL
-- eviction, after which the same labels survive on rows that no longer say anything about
-- its current home. Two observations separate the cases, either sufficing: the oldest
-- owner holds the identifier's latest sighting (a shared latest sighting counts as
-- holding it), or it first saw the identifier after every other owner already existed,
-- which an eviction cannot produce. An undated labelled owner makes "oldest" unverifiable
-- and two labelled owners sharing the oldest created_at leave it ambiguous; both refuse
-- the collapse rather than invent a winner. An undated or shared-timestamp owner among
-- the rest only disables the guard that needs its created_at.
, oldest_labelled as (
    select
        *,
        case when owner_merge_limited = 1 and owner_created_at is not null
              and owner_created_at = oldest_labelled_created_at then 1 else 0 end as is_oldest_labelled
    from stated
)
, guarded as (
    select
        *,
        sum(is_oldest_labelled) over (partition by id_type, id_value) as oldest_labelled_count,
        max(case when is_oldest_labelled = 1 then last_seen_at end) over (partition by id_type, id_value) as oldest_labelled_last_seen,
        max(case when is_oldest_labelled = 1 then first_seen_at end) over (partition by id_type, id_value) as oldest_labelled_first_seen,
        sum(case when is_oldest_labelled = 0 and owner_created_at is null then 1 else 0 end) over (partition by id_type, id_value) as undated_other_count,
        max(case when is_oldest_labelled = 0 then owner_created_at end) over (partition by id_type, id_value) as latest_other_created_at
    from oldest_labelled
)
, chained as (
    select
        *,
        case when merge_limited_owners > 1
              and unique_conflict_owners = 0
              and undated_labelled_count = 0
              and oldest_labelled_count = 1
              and (oldest_labelled_last_seen = latest_seen_at
                   or (undated_other_count = 0 and oldest_labelled_first_seen > latest_other_created_at))
             then 1 else 0 end as chained_collapse
    from guarded
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
    -- unranked exists to report: once the engine has named the owner, directly or through
    -- the guarded oldest-labelled collapse, there is nothing left for the timestamp
    -- tie-break to decide.
    case
        when identity_count = 1 then 'single'
        when merge_limited_owners = 1 and unique_conflict_owners = 0 then 'merge_limited'
        when chained_collapse = 1 then 'merge_limited'
        when undated_count > 0 or tied_count > 1 then 'unranked'
        else 'multiple'
    end as mapping_state,
    case
        when identity_count = 1 then true
        when merge_limited_owners = 1 and unique_conflict_owners = 0 then owner_merge_limited = 1
        when chained_collapse = 1 then is_oldest_labelled = 1
        {%- if var('snowplow__merge_limit_collapse', false) %}
        when undated_count > 0 or tied_count > 1 then false
        else owner_eligible = 1 and eligible_rank = 1 and eligible_tied_count = 1
        {%- else %}
        else false
        {%- endif %}
    end as is_preferred
from chained
