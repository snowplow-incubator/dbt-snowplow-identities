{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{#
  This run's identifier rows at their event-time grain: one row per
  (snowplow_id, id_type, id_value). Deliberately does NOT resolve to the active
  parent -- resolution happens at read time in snowplow_identities_identifier_mapping.
  Keeping this model unresolved is what makes the downstream table immutable: a row
  is written once under the snowplow_id that carried it and never re-pointed.
#}

{{
  config(
    tags=["this_run"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with hashed as (
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
        first_seen_event_id
    from {{ ref('snowplow_identities_new_identifiers_this_run') }}
    where snowplow_id is not null
)

-- Hashing lowers and trims before digesting, so distinct raw values can collapse
-- onto one hash. Re-aggregate to keep id_key unique for the downstream merge.
, ranked as (
    select
        snowplow_id,
        id_type,
        id_value,
        first_value(first_app_id) over (partition by snowplow_id, id_type, id_value order by first_derived_tstamp asc, first_seen_event_id asc) as first_app_id,
        first_value(last_app_id) over (partition by snowplow_id, id_type, id_value order by last_derived_tstamp desc, last_app_id desc, first_seen_event_id asc) as last_app_id,
        min(first_derived_tstamp) over (partition by snowplow_id, id_type, id_value) as first_seen_at,
        max(last_derived_tstamp) over (partition by snowplow_id, id_type, id_value) as last_seen_at,
        first_value(first_seen_event_id) over (partition by snowplow_id, id_type, id_value order by first_derived_tstamp asc, first_seen_event_id asc) as first_seen_event_id,
        row_number() over (partition by snowplow_id, id_type, id_value order by first_derived_tstamp asc, first_seen_event_id asc) as rn
    from hashed
)

select
    {{ dbt_utils.generate_surrogate_key(['snowplow_id', 'id_type', 'id_value']) }} as id_key,
    snowplow_id,
    id_type,
    id_value,
    first_app_id,
    last_app_id,
    first_seen_at,
    last_seen_at,
    first_seen_event_id
from ranked
where rn = 1
