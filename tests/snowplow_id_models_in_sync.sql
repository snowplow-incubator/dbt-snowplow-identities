{#
  Out-of-sync detection.

  With a shared manifest, the framework noticed when models had drifted apart and pulled
  them back into step. Self-watermarking has no such coordinator: each model advances off
  its own max(load_tstamp), so a model that errors for a week just quietly falls behind. It
  does catch up on its own, but nothing tells you it is behind in the meantime.

  This test is that signal. It reports every model whose watermark trails the
  furthest-ahead model by more than snowplow__max_watermark_drift_days, so drift surfaces
  as a test failure naming the lagging model rather than as unexplained gaps in the data.

  Some drift is normal and expected: models advance at most snowplow__backfill_limit_days
  per run and there is no barrier between them, so during a backfill they legitimately sit
  at different points. Keep the threshold comfortably above backfill_limit_days.

  snowplow_id_mapping is excluded when empty -- a pipeline with no merges yet has no rows
  to carry a watermark, which is not drift.

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
