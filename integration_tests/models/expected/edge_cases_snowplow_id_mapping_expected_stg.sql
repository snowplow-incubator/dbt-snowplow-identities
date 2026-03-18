{{ config(tags=['edge_cases']) }}

select *
from {{ ref('edge_cases_snowplow_id_mapping_expected') }}
