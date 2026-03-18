{{ config(tags=['complex_merges']) }}

select *
from {{ ref('complex_merges_new_identities_expected') }}
