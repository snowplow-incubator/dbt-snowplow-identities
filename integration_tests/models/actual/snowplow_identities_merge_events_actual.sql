select merge_event_id, active_snowplow_id, collector_tstamp, derived_tstamp
from {{ ref('snowplow_identities_merge_events') }}
