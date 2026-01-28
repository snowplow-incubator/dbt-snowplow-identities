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
      event_id as merge_event_id,
      unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1.snowplow_id AS active_snowplow_id,
      collector_tstamp,
      derived_tstamp,
      unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1.merged as merged,
      unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1.merges as merges
    from {{ ref('snowplow_identities_base_events_this_run') }}
    where event_name = 'identity_merge'
)

select
*
from prep
