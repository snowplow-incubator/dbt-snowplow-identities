{{ config(tags=['basic']) }}

select *
from {{ ref('basic_new_identities_expected') }}
