{{ config(tags=['complex_merges']) }}

select *
from {{ ref('complex_merges_id_changes_expected') }}
