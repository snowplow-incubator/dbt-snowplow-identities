{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{#
  The durable store behind snowplow_identities_identifier_mapping. One row per
  (snowplow_id, id_type, id_value), keyed on an immutable surrogate: id_key never
  changes, so a merge never rewrites, re-points, or deletes a row here. The only
  update a row ever receives is a widening of its first/last seen window when the
  same identifier is seen again under the same snowplow_id.

  Query snowplow_identities_identifier_mapping (the view) instead of this table --
  it resolves snowplow_id to the current active parent. This table is the raw,
  event-time record and is not the documented surface.
#}

{{ config(
    materialized="incremental",
    on_schema_change="append_new_columns",
    unique_key="id_key",
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    partition_by=snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "last_seen_at",
      "data_type": "timestamp"
    }, databricks_val='last_seen_at_date'),
    cluster_by=snowplow_identities.get_cluster_by_values('identifier_mapping_base'),
    tags=["derived"],
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    meta={'upsert_date_key': 'last_seen_at', 'snowplow_optimize': true}
) }}

with new_from_this_run as (
    select *
    from {{ ref('snowplow_identities_identifier_mapping_this_run') }}
    where {{ snowplow_utils.is_run_with_new_events('snowplow_identities') }}
)

{% if is_incremental() %}

-- Widen the seen-at window against the row already stored under this id_key,
-- keeping the app_id and event_id from whichever side actually saw it first/last.
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
             then t.first_seen_event_id else n.first_seen_event_id end as first_seen_event_id
    from new_from_this_run n
    left join {{ this }} t using (id_key)
)

select * from folded

{% else %}

select * from new_from_this_run

{% endif %}
