{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{#
  The durable store behind the identifier_mapping view. One row per
  (snowplow_id, id_type, id_value) under an immutable id_key, so merges never rewrite it.
  Query the view instead of this table unless you want the raw event-time record.
#}

{{ config(
    materialized="incremental",
    on_schema_change="append_new_columns",
    unique_key="id_key",
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    partition_by=snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "load_tstamp",
      "data_type": "timestamp"
    }, databricks_val='load_tstamp_date'),
    cluster_by=snowplow_identities.get_cluster_by_values('identifier_mapping_base'),
    tags=["derived"],
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    meta={'upsert_date_key': 'load_tstamp', 'snowplow_optimize': true}
) }}

{% set identifiers = var('snowplow__identifiers', [{'reference': 'domain_userid', 'alias': 'domain_userid'}, {'reference': 'user_id', 'alias': 'user_id'}]) %}
{% set identifier_columns = identifiers | map(attribute='alias') | list %}

with new_events as (
    select
        snowplow_id,
        {% for identifier in identifiers -%}
        {{ identifier.alias }},
        {% endfor -%}
        event_id,
        app_id,
        derived_tstamp,
        load_tstamp
    from {{ ref('snowplow_identities_stg_identity_events') }} s
    where {{ snowplow_identities.get_incremental_filter('s') }}
    and snowplow_id is not null
),

unpivoted as (
    select
        snowplow_id,
        col_name as id_type,
        {% if var('snowplow__hash_identifiers', false) -%}
        to_hex(sha256(lower(trim(id)))) as id_value,
        {%- else -%}
        id as id_value,
        {%- endif %}
        event_id,
        app_id,
        derived_tstamp,
        load_tstamp
    from new_events
    unpivot(id for col_name in ({{ identifier_columns | join(', ') }}))
    where id is not null
),

-- Aggregate after hashing: lower/trim can collapse distinct raw values onto one hash.
ranked as (
    select
        snowplow_id,
        id_type,
        id_value,
        first_value(app_id) over (partition by snowplow_id, id_type, id_value order by derived_tstamp asc, event_id asc) as first_app_id,
        first_value(app_id) over (partition by snowplow_id, id_type, id_value order by derived_tstamp desc, event_id desc) as last_app_id,
        min(derived_tstamp) over (partition by snowplow_id, id_type, id_value) as first_seen_at,
        max(derived_tstamp) over (partition by snowplow_id, id_type, id_value) as last_seen_at,
        first_value(event_id) over (partition by snowplow_id, id_type, id_value order by derived_tstamp asc, event_id asc) as first_seen_event_id,
        max(load_tstamp) over (partition by snowplow_id, id_type, id_value) as load_tstamp,
        row_number() over (partition by snowplow_id, id_type, id_value order by derived_tstamp asc, event_id asc) as rn
    from unpivoted
),

this_run as (
    select
        {{ dbt_utils.generate_surrogate_key(['snowplow_id', 'id_type', 'id_value']) }} as id_key,
        snowplow_id,
        id_type,
        id_value,
        first_app_id,
        last_app_id,
        first_seen_at,
        last_seen_at,
        first_seen_event_id,
        load_tstamp
    from ranked
    where rn = 1
)

{% if is_incremental() %}

-- Widen the seen-at window against the stored row, keeping the app_id and event_id from
-- whichever side saw it first/last. Joined on the natural key (equivalent to id_key, which
-- is derived from it) so the fold stays unit testable.
, folded as (
    select
        n.id_key,
        n.snowplow_id,
        n.id_type,
        n.id_value,
        case when t.first_seen_at is not null and t.first_seen_at <= n.first_seen_at
             then t.first_app_id else n.first_app_id end as first_app_id,
        case when t.last_seen_at is not null and t.last_seen_at >= n.last_seen_at
             then t.last_app_id else n.last_app_id end as last_app_id,
        least(n.first_seen_at, coalesce(t.first_seen_at, n.first_seen_at)) as first_seen_at,
        greatest(n.last_seen_at, coalesce(t.last_seen_at, n.last_seen_at)) as last_seen_at,
        case when t.first_seen_at is not null and t.first_seen_at <= n.first_seen_at
             then t.first_seen_event_id else n.first_seen_event_id end as first_seen_event_id,
        greatest(n.load_tstamp, coalesce(t.load_tstamp, n.load_tstamp)) as load_tstamp
    from this_run n
    left join {{ this }} t using (snowplow_id, id_type, id_value)
)

select * from folded

{% else %}

select * from this_run

{% endif %}
