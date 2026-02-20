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

with prep as (
    select
      event_id as merge_event_id
      {{ get_merge_fields() }},
      collector_tstamp,
      derived_tstamp,
    from {{ ref('snowplow_identities_base_events_this_run') }}
    where event_name = 'identity_merge'
)

select
    merge_event_id,
    active_snowplow_id,
    collector_tstamp,
    derived_tstamp,
    merged,
    merges
from prep
