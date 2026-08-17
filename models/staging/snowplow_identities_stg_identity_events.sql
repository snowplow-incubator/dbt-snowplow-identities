{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{#
  One row per event carrying the identity context, read straight from atomic.events and
  self-watermarked off its own max(load_tstamp).

  The configured identifier columns are kept un-pivoted here so that both consumers can
  derive their own aggregates from this one table: identities at the snowplow_id grain,
  identifier_mapping_base at the identifier grain. Pivoting here would force one of them
  to un-pivot again.
#}

{{ config(
    materialized="incremental",
    on_schema_change="append_new_columns",
    unique_key="event_id",
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    partition_by=snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "load_tstamp",
      "data_type": "timestamp"
    }, databricks_val='load_tstamp_date'),
    cluster_by=snowplow_identities.get_cluster_by_values('stg_identity_events'),
    tags=["staging"],
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    meta={'upsert_date_key': 'load_tstamp', 'snowplow_optimize': true}
) }}

{% set identifiers = var('snowplow__identifiers', [{'reference': 'domain_userid', 'alias': 'domain_userid'}, {'reference': 'user_id', 'alias': 'user_id'}]) %}

with prep as (
    select
        {{ snowplow_identities.get_identity_fields() }}
        {% for identifier in identifiers -%}
        {{ identifier.reference }} as {{ identifier.alias }},
        {% endfor -%}
        event_id,
        app_id,
        derived_tstamp,
        collector_tstamp,
        load_tstamp
    from {{ source('atomic', 'events') }}
    where
    {% if target.type == 'bigquery' %}
        {{ snowplow_utils.get_optional_fields(
            enabled=true,
            col_prefix='contexts_com_snowplowanalytics_snowplow_identity_2',
            fields=[{'field': ('snowplow_id', 'snowplow_id'), 'dtype': 'string'}],
            relation=source('atomic', 'events'),
            relation_alias=none,
            include_field_alias=false
        ) }} is not null
    {% else %}
        contexts_com_snowplowanalytics_snowplow_identity_2 is not null
    {% endif %}
    and {{ snowplow_identities.get_incremental_filter(use_atomic_partition=true) }}
    {%- if var('snowplow__app_id', []) | length > 0 %}
    and app_id in ({% for aid in var('snowplow__app_id', []) %}'{{ aid }}'{% if not loop.last %}, {% endif %}{% endfor %})
    {%- endif %}
    qualify row_number() over (partition by event_id order by collector_tstamp) = 1
)

select
    snowplow_id,
    created_at,
    {% for identifier in identifiers -%}
    {{ identifier.alias }},
    {% endfor -%}
    event_id,
    app_id,
    derived_tstamp,
    collector_tstamp,
    load_tstamp
from prep
