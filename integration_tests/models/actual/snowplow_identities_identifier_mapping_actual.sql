select active_snowplow_id, id_type, id_value, first_app_id, last_app_id, first_seen_at, last_seen_at, first_seen_event_id, mapping_state, is_preferred
from {{ ref('snowplow_identities_identifier_mapping') }}
