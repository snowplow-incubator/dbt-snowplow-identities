{{ config(
    materialized='table',
    schema='snplw_identities_int_tests'
) }}

{{ create_events_stg() }}
