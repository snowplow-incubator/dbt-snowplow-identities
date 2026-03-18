{{ config(tags=['complex_merges']) }}

select *
from {{ ref('complex_merges_snowplow_id_mapping_expected') }}
