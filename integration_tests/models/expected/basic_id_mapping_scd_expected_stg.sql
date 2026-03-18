{{ config(tags=['basic']) }}

select
    snowplow_id,
    active_snowplow_id,
    effective_at,
    superseded_at,
    change_type,
    cast(is_current as boolean) as is_current
from {{ ref('basic_id_mapping_scd_expected') }}
