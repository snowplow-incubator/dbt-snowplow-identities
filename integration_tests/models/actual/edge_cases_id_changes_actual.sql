{{ config(tags=['edge_cases']) }}

select
    snowplow_id,
    previous_snowplow_id,
    effective_at,
    change_type,
    first_seen_event_id,
    first_seen_app_id
from {{ ref('snowplow_identities_id_changes') }}
