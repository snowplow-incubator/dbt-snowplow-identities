-- Invariant: no row in identifier_mapping should point to a merged-away identity.
-- If active_snowplow_id appears as snowplow_id in snowplow_id_mapping,
-- that identity has been absorbed and the row is stale.
--
-- Now that identifier_mapping is a view resolving at read time, this should hold by
-- construction. Kept as an end-to-end guard: it fails if the view stops resolving,
-- or if snowplow_id_mapping develops chains (see snowplow_id_mapping_no_chains.sql).
-- This test returns rows that violate the invariant (should return 0).

select
    im.id_type,
    im.id_value,
    im.active_snowplow_id as stale_active_id,
    m.active_snowplow_id as correct_active_id
from {{ ref('snowplow_identities_identifier_mapping') }} im
inner join {{ ref('snowplow_identities_snowplow_id_mapping') }} m
    on im.active_snowplow_id = m.snowplow_id
