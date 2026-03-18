{{ config(tags=['basic']) }}

select
    snowplow_id,
    active_snowplow_id,
    merged_at
from {{ ref('snowplow_identities_snowplow_id_mapping') }}
