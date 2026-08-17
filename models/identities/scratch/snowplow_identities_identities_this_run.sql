{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{#
  One row per snowplow_id seen this run: when the identity was created, and where it was
  first and last seen. Per-identifier columns are deliberately absent -- they live in
  snowplow_identities_identifier_mapping at the identifier grain, where they are complete
  and always current rather than collapsed to one value per identity.
#}

with prep as (
    select
        -- Extract identity fields directly from the event using the macro
        {{ snowplow_identities.get_identity_fields() }}
        event_id,
        app_id,
        derived_tstamp,
        collector_tstamp
    from {{ ref('snowplow_identities_base_events_this_run') }}
    where
    {% if target.type == 'bigquery' %}
        {{ snowplow_utils.get_optional_fields(
            enabled=true,
            col_prefix='contexts_com_snowplowanalytics_snowplow_identity_2',
            fields=[{'field': ('snowplow_id', 'snowplow_id'), 'dtype': 'string'}],
            relation=source('atomic', 'events') if 'integration_tests' in project_name and 'snowplow' in project_name else source('atomic', 'events'),
            relation_alias=none,
            include_field_alias=false
        ) }} is not null
    {% else %}
        contexts_com_snowplowanalytics_snowplow_identity_2 is not null
    {% endif %}
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

select
    f.snowplow_id,
    f.created_at,
    f.event_id as first_seen_event_id,
    f.app_id as first_app_id,
    l.app_id as last_app_id,
    f.derived_tstamp as first_derived_tstamp,
    l.derived_tstamp as last_derived_tstamp
from first_event f
left join last_event l using (snowplow_id)
