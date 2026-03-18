{{ config(tags=['edge_cases']) }}

select
    active_snowplow_id,
    lower(id_type) as id_type,
    id_value,
    first_app_id,
    last_app_id,
    first_seen_at,
    last_seen_at,
    first_seen_event_id
from {{ ref('snowplow_identities_identifier_mapping') }}
