{#
  Out-of-sync detection. Without a shared manifest, a model that errors just quietly falls
  behind -- it self-heals, but nothing tells you. Reports any model whose watermark trails
  the furthest-ahead model by more than snowplow__max_watermark_drift_days.

  Some drift is normal during a backfill, so keep the threshold above
  snowplow__backfill_limit_days. snowplow_id_mapping is skipped when empty (no merges yet).
  Returns rows that violate the invariant (should return 0).
#}

with watermarks as (
    select 'snowplow_identities_stg_identity_events' as model_name, max(load_tstamp) as watermark
    from {{ ref('snowplow_identities_stg_identity_events') }}
    union all
    select 'snowplow_identities_merge_events', max(load_tstamp)
    from {{ ref('snowplow_identities_merge_events') }}
    union all
    select 'snowplow_identities_identities', max(load_tstamp)
    from {{ ref('snowplow_identities_identities') }}
    union all
    select 'snowplow_identities_identifier_mapping_base', max(load_tstamp)
    from {{ ref('snowplow_identities_identifier_mapping_base') }}
    union all
    select 'snowplow_identities_snowplow_id_mapping', max(load_tstamp)
    from {{ ref('snowplow_identities_snowplow_id_mapping') }}
),

populated as (
    select model_name, watermark
    from watermarks
    where watermark is not null
),

furthest_ahead as (
    select max(watermark) as leading_watermark
    from populated
)

select
    p.model_name,
    p.watermark,
    f.leading_watermark,
    {{ dbt.datediff('p.watermark', 'f.leading_watermark', 'day') }} as days_behind
from populated p
cross join furthest_ahead f
where {{ dbt.datediff('p.watermark', 'f.leading_watermark', 'day') }} > {{ var('snowplow__max_watermark_drift_days', 7) }}
