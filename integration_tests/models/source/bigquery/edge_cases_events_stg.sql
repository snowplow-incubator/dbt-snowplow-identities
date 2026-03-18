with prep as (
    select
        * except (
            contexts_com_snowplowanalytics_snowplow_identity_1,
            unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1
        ),
        JSON_EXTRACT_ARRAY(
            contexts_com_snowplowanalytics_snowplow_identity_1
        ) as _identity_json,
        JSON_EXTRACT(
            unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1, '$'
        ) as _merge_json
    from {{ ref('edge_cases_events') }}
)

select
    * except (_identity_json, _merge_json),

    -- Reconstruct identity context as ARRAY<STRUCT<snowplow_id, created_at>>
    array(
        select as struct
            JSON_EXTRACT_SCALAR(j, '$.snowplowId') as snowplow_id,
            cast(JSON_EXTRACT_SCALAR(j, '$.createdAt') as timestamp) as created_at
        from unnest(_identity_json) as j
    ) as contexts_com_snowplowanalytics_snowplow_identity_1,

    -- Reconstruct merge event as struct with nested array
    struct(
        JSON_EXTRACT_SCALAR(_merge_json, '$.snowplowId') as snowplow_id,
        array(
            select as struct
                JSON_EXTRACT_SCALAR(m, '$.snowplowId') as snowplow_id,
                cast(JSON_EXTRACT_SCALAR(m, '$.mergedAt') as timestamp) as merged_at,
                JSON_EXTRACT_SCALAR(m, '$.triggeringEventId') as triggering_event_id
            from unnest(JSON_EXTRACT_ARRAY(_merge_json, '$.merged')) as m
        ) as merged,
        JSON_EXTRACT_STRING_ARRAY(_merge_json, '$.merges') as merges
    ) as unstruct_event_com_snowplowanalytics_snowplow_identity_merge_1

from prep
