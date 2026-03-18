{{ config(tags=['complex_merges']) }}

select *
from {{ ref('complex_merges_identifier_mapping_expected') }}
