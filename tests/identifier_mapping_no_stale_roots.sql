-- Invariant: no identifier_mapping row should point at a merged-away identity. True by
-- construction now that the view resolves at read time; kept as an end-to-end guard.
-- Returns rows that violate the invariant (should return 0).

select
    im.id_type,
    im.id_value,
    im.active_snowplow_id as stale_active_id,
    m.active_snowplow_id as correct_active_id
from {{ ref('snowplow_identities_identifier_mapping') }} im
inner join {{ ref('snowplow_identities_snowplow_id_mapping') }} m
    on im.active_snowplow_id = m.snowplow_id
