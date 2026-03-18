{{ config(tags=['edge_cases']) }}

select *
from {{ ref('edge_cases_identifier_mapping_expected') }}
