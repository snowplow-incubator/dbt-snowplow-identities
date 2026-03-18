{{ config(tags=['basic']) }}

select *
from {{ ref('basic_identifier_mapping_expected') }}
