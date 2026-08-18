-- Invariant: snowplow_id_mapping must be flat, never chained. If an active_snowplow_id also
-- appears as a snowplow_id, resolving a child lands on an intermediate rather than the final
-- root, which breaks identifier_mapping's single-hop resolution.
-- Returns rows that violate the invariant (should return 0).

select
    parent.snowplow_id as intermediate_id,
    parent.active_snowplow_id as final_root,
    child.snowplow_id as orphaned_child
from {{ ref('snowplow_identities_snowplow_id_mapping') }} child
inner join {{ ref('snowplow_identities_snowplow_id_mapping') }} parent
    on child.active_snowplow_id = parent.snowplow_id
