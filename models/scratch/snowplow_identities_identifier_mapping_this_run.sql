{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{
  config(
    tags=["this_run"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

{% set identifiers = var('snowplow__identifiers', [{'reference': 'domain_userid', 'alias': 'domain_userid'}, {'reference': 'user_id', 'alias': 'user_id'}]) %}
{% set identifier_columns = identifiers | map(attribute='alias') | list %}

-- Step 1: Get new identifiers from new identities in this run
with prep as (
    SELECT
        snowplow_id,
        col_name AS id_type,
        {% if var('snowplow__hash_identifiers', false) %}
            to_hex(sha256(lower(trim(id)))) as id_value,
        {% else %}
            id as id_value,
        {% endif %}
        first_app_id,
        last_app_id,
        first_derived_tstamp,
        last_derived_tstamp,
        first_seen_event_id
    FROM {{ ref('snowplow_identities_new_identities_this_run') }}
    UNPIVOT(id FOR col_name IN ({{ identifier_columns | join(', ') }}))
    WHERE id IS NOT NULL
)

, new_identifiers as (
    select
        snowplow_id,
        id_type,
        id_value,
        first_app_id,
        last_app_id,
        first_derived_tstamp,
        last_derived_tstamp,
        first_seen_event_id,
        {{ dbt_utils.generate_surrogate_key(['id_type', 'id_value']) }} as uuid
    from prep
)

-- Step 2: Get existing identifiers that need active_snowplow_id updates due to merges
{% if is_incremental() %}
, merged_ids_this_run as (
    select distinct snowplow_id
    from {{ ref('snowplow_identities_snowplow_id_mapping_this_run') }}
)

, existing_identifiers_to_update as (
    select
        hist.id_type,
        hist.id_value,
        hist.uuid,
        hist.first_app_id,
        hist.last_app_id,
        hist.first_seen_at,
        hist.last_seen_at,
        hist.first_seen_event_id,
        hist.active_snowplow_id as snowplow_id
    from {{ this }} hist
    where hist.active_snowplow_id in (select snowplow_id from merged_ids_this_run)
    -- Exclude any that are already being updated via new_identifiers
    and not exists (
        select 1 from new_identifiers n where n.uuid = hist.uuid
    )
)

, all_identifiers as (
    -- New identifiers from this run
    select
        snowplow_id,
        id_type,
        id_value,
        uuid,
        first_app_id,
        last_app_id,
        first_derived_tstamp as first_seen_at,
        last_derived_tstamp as last_seen_at,
        first_seen_event_id
    from new_identifiers

    union all

    -- Existing identifiers that need updates
    select
        snowplow_id,
        id_type,
        id_value,
        uuid,
        first_app_id,
        last_app_id,
        first_seen_at,
        last_seen_at,
        first_seen_event_id
    from existing_identifiers_to_update
)
{% else %}
, all_identifiers as (
    select
        snowplow_id,
        id_type,
        id_value,
        uuid,
        first_app_id,
        last_app_id,
        first_derived_tstamp as first_seen_at,
        last_derived_tstamp as last_seen_at,
        first_seen_event_id
    from new_identifiers
)
{% endif %}

-- Step 3: Resolve to current active_snowplow_id and merge timestamps
, with_current_mapping as (
    select
        a.id_type,
        a.id_value,
        a.uuid,
        a.first_app_id,
        a.last_app_id,
        a.first_seen_at,
        a.last_seen_at,
        a.first_seen_event_id,
        coalesce(id_map.active_snowplow_id, a.snowplow_id) as active_snowplow_id
    from all_identifiers a
    left join {{ ref('snowplow_identities_snowplow_id_mapping') }} id_map
        on a.snowplow_id = id_map.snowplow_id
    where a.snowplow_id is not null
)

{% if is_incremental() %}
-- Step 4: Merge with historical data to get correct first/last seen timestamps
-- Aggregate in case same identifier appears multiple times due to multiple merges
, with_historical_timestamps as (
    select
        c.active_snowplow_id,
        c.id_type,
        c.id_value,
        c.uuid,
        c.first_app_id,
        c.last_app_id,
        least(c.first_seen_at, coalesce(h.first_seen_at, c.first_seen_at)) as first_seen_at,
        greatest(c.last_seen_at, coalesce(h.last_seen_at, c.last_seen_at)) as last_seen_at,
        coalesce(h.first_seen_event_id, c.first_seen_event_id) as first_seen_event_id
    from with_current_mapping c
    left join {{ this }} h
        on c.uuid = h.uuid
)

, aggregated as (
    select
        active_snowplow_id,
        id_type,
        id_value,
        uuid,
        any_value(first_app_id) as first_app_id,
        any_value(last_app_id) as last_app_id,
        min(first_seen_at) as first_seen_at,
        max(last_seen_at) as last_seen_at,
        any_value(first_seen_event_id) as first_seen_event_id
    from with_historical_timestamps
    group by active_snowplow_id, id_type, id_value, uuid
)

select
    uuid,
    active_snowplow_id,
    id_type,
    id_value,
    first_app_id,
    last_app_id,
    first_seen_at,
    last_seen_at,
    first_seen_event_id
from aggregated
{% else %}
, aggregated as (
    select
        active_snowplow_id,
        id_type,
        id_value,
        uuid,
        any_value(first_app_id) as first_app_id,
        any_value(last_app_id) as last_app_id,
        min(first_seen_at) as first_seen_at,
        max(last_seen_at) as last_seen_at,
        any_value(first_seen_event_id) as first_seen_event_id
    from with_current_mapping
    group by active_snowplow_id, id_type, id_value, uuid
)

select
    uuid,
    active_snowplow_id,
    id_type,
    id_value,
    first_app_id,
    last_app_id,
    first_seen_at,
    last_seen_at,
    first_seen_event_id
from aggregated
{% endif %}
