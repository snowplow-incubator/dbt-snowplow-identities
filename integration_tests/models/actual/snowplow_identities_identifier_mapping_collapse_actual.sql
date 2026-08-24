{# Compared against the expected rows for runs with snowplow__merge_limit_collapse
   on. The SIC, UIDC and MTUID scenarios are excluded because they configure a
   unique identifier, which the var is not valid for; their rows are asserted in
   the runs with the var off. #}
select active_snowplow_id, upper(id_type) as id_type, id_value, first_app_id, last_app_id, first_seen_at, last_seen_at, first_seen_event_id
from {{ ref('snowplow_identities_identifier_mapping') }}
where id_value not like '%SIC%'
  and id_value not like '%UIDC%'
  and id_value not like '%MTUID%'
