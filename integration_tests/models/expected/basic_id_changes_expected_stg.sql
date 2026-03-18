{{ config(tags=['basic']) }}

select *
from {{ ref('basic_id_changes_expected') }}
