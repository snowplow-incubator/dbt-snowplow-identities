select snowplow_id, active_snowplow_id, effective_at, superseded_at, change_type, is_current
from {{ ref('snowplow_identities_id_mapping_scd_expected') }}
