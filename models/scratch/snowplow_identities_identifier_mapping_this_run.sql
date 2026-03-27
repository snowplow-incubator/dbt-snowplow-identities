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

with new_identifiers as (
    select
        snowplow_id,
        id_type,
        {% if var('snowplow__hash_identifiers', false) %}
            to_hex(sha256(lower(trim(id_value)))) as id_value,
        {% else %}
            id_value,
        {% endif %}
        first_app_id,
        last_app_id,
        first_derived_tstamp,
        last_derived_tstamp,
        first_seen_event_id,
        {{ dbt_utils.generate_surrogate_key(['id_type', 'id_value']) }} as uuid
    from {{ ref('snowplow_identities_new_identifiers_this_run') }}
)

, with_current_mapping as (
    select
        a.id_type,
        a.id_value,
        a.uuid,
        a.first_app_id,
        a.last_app_id,
        a.first_derived_tstamp as first_seen_at,
        a.last_derived_tstamp as last_seen_at,
        a.first_seen_event_id,
        coalesce(id_map.active_snowplow_id, a.snowplow_id) as active_snowplow_id
    from new_identifiers a
    left join {{ ref('snowplow_identities_snowplow_id_mapping') }} id_map
        on a.snowplow_id = id_map.snowplow_id
    where a.snowplow_id is not null
)

select
    uuid,
    active_snowplow_id,
    id_type,
    id_value,
    any_value(first_app_id) as first_app_id,
    any_value(last_app_id) as last_app_id,
    min(first_seen_at) as first_seen_at,
    max(last_seen_at) as last_seen_at,
    any_value(first_seen_event_id) as first_seen_event_id
from with_current_mapping
group by active_snowplow_id, id_type, id_value, uuid
