select active_snowplow_id, id_type, id_value, first_app_id, last_app_id, first_seen_at, last_seen_at, first_seen_event_id, mapping_state, {{ 'is_preferred_with_collapse' if var('snowplow__merge_limit_collapse', false) else 'is_preferred' }} as is_preferred
from {{ ref('snowplow_identities_identifier_mapping_expected') }}

{% if var('snowplow__merge_limit_collapse', false) %}
-- Unique-identifier scenarios are outside this opt-in heuristic's contract.
where id_value not like '%SIC%'
  and id_value not like '%UIDC%'
  and id_value not like '%MTUID%'
{% endif %}
