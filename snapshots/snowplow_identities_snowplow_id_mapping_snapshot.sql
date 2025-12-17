{% snapshot snowplow_identities_id_mapping_snapshot %}

{{
    config(
      unique_key='snowplow_id',
      strategy='timestamp',
      updated_at='updated_at',
      target_database=target.database,
      target_schema=target.schema ~ '_derived'
    )
}}

select * from {{ ref('snowplow_identities_snowplow_id_mapping') }}

{% endsnapshot %}
