select
    * exclude (
        contexts_com_snowplowanalytics_snowplow_identity_1,
        unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1
    ),
    parse_json(contexts_com_snowplowanalytics_snowplow_identity_1)
        as contexts_com_snowplowanalytics_snowplow_identity_1,
    parse_json(unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1)
        as unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1
from {{ ref('basic_events') }}
