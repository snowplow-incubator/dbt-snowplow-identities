{{ config(tags=['basic']) }}

select
    snowplow_id,
    created_at,
    first_seen_event_id,
    first_app_id,
    last_app_id,
    domain_userid,
    user_id,
    first_derived_tstamp,
    last_derived_tstamp
from {{ ref('snowplow_identities_new_identities') }}
