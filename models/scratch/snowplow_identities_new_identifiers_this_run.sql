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

with events as (
    select
        {{ snowplow_identities.get_identity_fields() }}
        {% for identifier in identifiers %}
        {{ identifier.reference }} as {{ identifier.alias }},
        {% endfor %}
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

, unpivoted as (
    SELECT
        snowplow_id,
        col_name AS id_type,
        id AS id_value,
        event_id,
        app_id,
        derived_tstamp
    FROM events
    UNPIVOT(id FOR col_name IN ({{ identifier_columns | join(', ') }}))
    WHERE id IS NOT NULL
)

, first_occurrence as (
    select *
    from unpivoted
    qualify row_number() over (
        partition by snowplow_id, id_type, id_value
        order by derived_tstamp asc, event_id asc
    ) = 1
)

, last_occurrence as (
    select *
    from unpivoted
    qualify row_number() over (
        partition by snowplow_id, id_type, id_value
        order by derived_tstamp desc, event_id desc
    ) = 1
)

select
    f.snowplow_id,
    f.id_type,
    f.id_value,
    f.event_id as first_seen_event_id,
    f.app_id as first_app_id,
    l.app_id as last_app_id,
    f.derived_tstamp as first_derived_tstamp,
    l.derived_tstamp as last_derived_tstamp
from first_occurrence f
left join last_occurrence l using (snowplow_id, id_type, id_value)
