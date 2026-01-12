{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{
  config(
    materialized="incremental",
    on_schema_change='append_new_columns',
    unique_key='id_change_key',
    upsert_date_key='effective_at',
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "effective_at",
      "data_type": "timestamp"
    }, databricks_val='effective_at_date'),
    cluster_by=snowplow_identities.get_cluster_by_values('id_changes'),
    tags=["derived"],
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    snowplow_optimize=true
  )
}}

select *

from {{ ref('snowplow_identities_id_changes_this_run') }}
where {{ snowplow_utils.is_run_with_new_events('snowplow_identities') }}
