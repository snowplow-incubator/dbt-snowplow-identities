{{ config(tags=['basic']) }}

select *
from {{ ref('basic_snowplow_id_mapping_expected') }}
