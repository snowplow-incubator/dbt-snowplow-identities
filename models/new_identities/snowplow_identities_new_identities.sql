{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{ config(
    materialized="incremental",
    on_schema_change="append_new_columns",
    unique_key="snowplow_id",
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    partition_by=snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "first_derived_tstamp",
      "data_type": "timestamp"
    }, databricks_val='first_derived_tstamp_date'),
    cluster_by=snowplow_identities.get_cluster_by_values('new_identities'),
    tags=["derived"],
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    meta={'upsert_date_key': 'first_derived_tstamp', 'snowplow_optimize': true}
) }}

{% if is_incremental() %}

with new_data as (
    select *
    from {{ ref('snowplow_identities_new_identities_this_run') }}
    where {{ snowplow_utils.is_run_with_new_events('snowplow_identities') }}
),

merged as (
    select
        n.*,
        t.snowplow_id is not null as existed_before,
        t.first_derived_tstamp < n.first_derived_tstamp as old_is_earlier,
        t.last_derived_tstamp > n.last_derived_tstamp as old_is_later,
        t.created_at as prev_created_at,
        t.first_seen_event_id as prev_first_seen_event_id,
        t.first_app_id as prev_first_app_id,
        t.last_app_id as prev_last_app_id,
        {% for identifier in var('snowplow__identifiers', [{'reference': 'domain_userid', 'alias': 'domain_userid'}, {'reference': 'user_id', 'alias': 'user_id'}]) %}
        t.{{ identifier.alias }} as prev_{{ identifier.alias }},
        {% endfor %}
        t.first_derived_tstamp as prev_first_derived_tstamp,
        t.last_derived_tstamp as prev_last_derived_tstamp
    from new_data n
    left join {{ this }} t using (snowplow_id)
)

select
    snowplow_id,
    case when old_is_earlier then prev_created_at else created_at end as created_at,
    case when old_is_earlier then prev_first_seen_event_id else first_seen_event_id end as first_seen_event_id,
    case when old_is_earlier then prev_first_app_id else first_app_id end as first_app_id,
    case when old_is_later then prev_last_app_id else last_app_id end as last_app_id,
    {% for identifier in var('snowplow__identifiers', [{'reference': 'domain_userid', 'alias': 'domain_userid'}, {'reference': 'user_id', 'alias': 'user_id'}]) %}
    coalesce({{ identifier.alias }}, prev_{{ identifier.alias }}) as {{ identifier.alias }},
    {% endfor %}
    case when old_is_earlier then prev_first_derived_tstamp else first_derived_tstamp end as first_derived_tstamp,
    case when old_is_later then prev_last_derived_tstamp else last_derived_tstamp end as last_derived_tstamp
from merged

{% else %}

select *
from {{ ref('snowplow_identities_new_identities_this_run') }}
where {{ snowplow_utils.is_run_with_new_events('snowplow_identities') }}

{% endif %}
