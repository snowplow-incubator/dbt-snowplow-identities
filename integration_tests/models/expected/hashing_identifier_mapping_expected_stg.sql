{{ config(tags=['hashing']) }}

select *
from {{ ref('hashing_identifier_mapping_expected') }}
