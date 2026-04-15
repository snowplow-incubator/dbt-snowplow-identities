{% macro snowflake__create_events_stg() %}

select
    event_id,
    event_name,
    app_id,
    domain_userid,
    user_id,
    network_userid,
    collector_tstamp,
    derived_tstamp,
    dvce_created_tstamp,
    load_tstamp,
    try_parse_json(contexts_com_snowplowanalytics_snowplow_identity_2) as contexts_com_snowplowanalytics_snowplow_identity_2,
    try_parse_json(unstruct_event_com_snowplowanalytics_snowplow_identity_merge_2) as unstruct_event_com_snowplowanalytics_snowplow_identity_merge_2
from {{ ref('snowplow_identities_events') }}

{% endmacro %}

{% macro bigquery__create_events_stg() %}

select
    event_id,
    event_name,
    app_id,
    domain_userid,
    user_id,
    network_userid,
    collector_tstamp,
    derived_tstamp,
    dvce_created_tstamp,
    load_tstamp,
    case
        when contexts_com_snowplowanalytics_snowplow_identity_2 is not null
        then array(
            select as struct
                json_value(item, '$.snowplow_id') as snowplow_id,
                cast(json_value(item, '$.created_at') as timestamp) as created_at
            from unnest(json_extract_array(contexts_com_snowplowanalytics_snowplow_identity_2)) as item
        )
        else null
    end as contexts_com_snowplowanalytics_snowplow_identity_2,
    case
        when unstruct_event_com_snowplowanalytics_snowplow_identity_merge_2 is not null
        then struct(
            json_value(unstruct_event_com_snowplowanalytics_snowplow_identity_merge_2, '$.snowplow_id') as snowplow_id,
            array(
                select as struct
                    json_value(item, '$.snowplow_id') as snowplow_id,
                    cast(json_value(item, '$.created_at') as timestamp) as created_at,
                    cast(json_value(item, '$.merged_at') as timestamp) as merged_at,
                    json_value(item, '$.triggering_event_id') as triggering_event_id
                from unnest(json_extract_array(unstruct_event_com_snowplowanalytics_snowplow_identity_merge_2, '$.merged')) as item
            ) as merged,
            array(
                select json_value(item, '$')
                from unnest(json_extract_array(unstruct_event_com_snowplowanalytics_snowplow_identity_merge_2, '$.merges')) as item
            ) as merges
        )
        else null
    end as unstruct_event_com_snowplowanalytics_snowplow_identity_merge_2
from {{ ref('snowplow_identities_events') }}

{% endmacro %}

{% macro create_events_stg() %}
    {{ return(adapter.dispatch('create_events_stg', 'snowplow_identities_integration_tests')()) }}
{% endmacro %}
