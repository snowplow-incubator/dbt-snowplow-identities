{{ config(tags=['edge_cases']) }}

select *
from {{ ref('edge_cases_new_identities_expected') }}
