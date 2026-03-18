{{ config(tags=['edge_cases']) }}

select *
from {{ ref('edge_cases_id_changes_expected') }}
