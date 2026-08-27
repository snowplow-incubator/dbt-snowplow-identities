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
        b.first_seen_event_id
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
        first_seen_event_id
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
-- after its state has expired. Those two are indistinguishable from here, so name the
-- shape rather than the cause and pick a row only when the oldest identity is also the
-- one the identifier was last seen under.
, ages_joined as (
    select
        d.*,
        count(*) over (partition by d.id_type, d.id_value) as identity_count,
        sum(case when a.created_at is null then 1 else 0 end) over (partition by d.id_type, d.id_value) as undated_count,
        row_number() over (partition by d.id_type, d.id_value order by a.created_at asc nulls last, d.active_snowplow_id asc) as age_rn,
        first_value(d.active_snowplow_id) over (partition by d.id_type, d.id_value order by d.last_seen_at desc, a.created_at asc nulls last, d.active_snowplow_id asc rows between unbounded preceding and current row) as last_seen_snowplow_id
    from deduped d
    left join identity_ages a
        on d.active_snowplow_id = a.snowplow_id
)

, stated as (
    select
        *,
        max(case when age_rn = 1 then active_snowplow_id end) over (partition by id_type, id_value) as oldest_snowplow_id
    from ages_joined
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
    case
        when identity_count = 1 then 'single'
        when undated_count = 0 and oldest_snowplow_id = last_seen_snowplow_id then 'multiple'
        else 'unranked'
    end as mapping_state,
    case
        when identity_count = 1 then true
        {%- if var('snowplow__merge_limit_collapse', false) %}
        when undated_count = 0 and oldest_snowplow_id = last_seen_snowplow_id then age_rn = 1
        {%- endif %}
        else false
    end as is_preferred
from stated
