{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

with prep as (
    select
        -- Extract identity fields directly from the event using the macro
        {{ snowplow_identities.get_identity_fields() }}
        {% for identifier in var('snowplow__identifiers', [{'reference': 'domain_userid', 'alias': 'domain_userid'}, {'reference': 'user_id', 'alias': 'user_id'}]) %}
        {{ identifier.alias }},
        {% endfor %}
        event_id,
        app_id,
        derived_tstamp,
        collector_tstamp
    from {{ ref('snowplow_identities_base_events_this_run') }}
    where contexts_com_snowplowanalytics_snowplow_identity_1 is not null
    qualify row_number() over (partition by event_id order by collector_tstamp) = 1
)

, first_event as (
    select *
    from prep
    QUALIFY row_number() over (
        partition by snowplow_id
        order by derived_tstamp asc, event_id asc
    ) = 1
)

, last_event as (
    select *
    from prep
    QUALIFY row_number() over (
        partition by snowplow_id
        order by derived_tstamp desc, event_id desc
    ) = 1
)

, aggregated_values as (
    -- Aggregate all identifier values across all events for each snowplow_id
    -- This ensures we capture identifiers even if they don't appear in the first event
    select
        snowplow_id,
        {% for identifier in var('snowplow__identifiers', [{'reference': 'domain_userid', 'alias': 'domain_userid'}, {'reference': 'user_id', 'alias': 'user_id'}]) %}
            max({{ identifier.alias }}) as {{ identifier.alias }}{% if not loop.last %},{% endif %}
        {% endfor %}
        -- Add more identifiers here: MAX(email) as email, MAX(phone) as phone, etc.
    from prep
    group by snowplow_id
)

select
    f.snowplow_id,
    f.created_at,
    f.event_id as first_seen_event_id,
    f.app_id as first_app_id,
    l.app_id as last_app_id,
    {% for identifier in var('snowplow__identifiers', [{'reference': 'domain_userid', 'alias': 'domain_userid'}, {'reference': 'user_id', 'alias': 'user_id'}]) %}
    a.{{ identifier.alias }},
    {% endfor %}
    f.derived_tstamp as first_derived_tstamp,
    l.derived_tstamp as last_derived_tstamp
from first_event f
left join last_event l using (snowplow_id)
left join aggregated_values a using (snowplow_id)
