-- Invariant: snowplow_id_mapping must be flat, never chained.
-- If an active_snowplow_id also appears as a snowplow_id, then resolving a child
-- through this table lands on an intermediate rather than the final root.
--
-- snowplow_identities_identifier_mapping resolves exactly one hop, so it depends on
-- this holding. The true_parents filter in snowplow_id_mapping is what
-- guarantees it; this test is the guard.
-- Returns rows that violate the invariant (should return 0).

select
    parent.snowplow_id as intermediate_id,
    parent.active_snowplow_id as final_root,
    child.snowplow_id as orphaned_child
from {{ ref('snowplow_identities_snowplow_id_mapping') }} child
inner join {{ ref('snowplow_identities_snowplow_id_mapping') }} parent
    on child.active_snowplow_id = parent.snowplow_id
