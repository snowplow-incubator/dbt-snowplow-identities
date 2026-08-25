# Migration guide

## Upgrading to 0.2.0

This release removes two output tables, renames a third, and rebuilds
`snowplow_identities_identifier_mapping` as a view. That view keeps the old name, columns and
grain, so most consumers need no changes.

### What changed

| Table | Change | Action needed |
| --- | --- | --- |
| `snowplow_identities_snowplow_id_mapping` | Same grain and values. Gains `merge_event_id`, `triggering_event_id` and `load_tstamp`; now partitioned on `load_tstamp` rather than `merged_at` | Drop the table first; see [Tables you must drop before the first run](#tables-you-must-drop-before-the-first-run) |
| `snowplow_identities_merge_events` | Same grain and values. Gains `load_tstamp`; now partitioned on `load_tstamp` rather than `derived_tstamp` | Drop the table first; see [Tables you must drop before the first run](#tables-you-must-drop-before-the-first-run) |
| `snowplow_identities_identifier_mapping` | Now a **view** over a new table. Same columns, same grain, same values. | None for consumers |
| `snowplow_identities_new_identities` | Renamed to `snowplow_identities_identities`; per-identifier columns removed | Update references; see [Rebuilding the wide identity row](#rebuilding-the-wide-identity-row) |
| `snowplow_identities_id_changes` | **Removed** | See [Rebuilding id_changes](#rebuilding-id_changes) |
| `snowplow_identities_id_mapping_scd` | **Removed** | See [Rebuilding id_mapping_scd](#rebuilding-id_mapping_scd) |

New models:

- `snowplow_identities_identifier_mapping_base` (`derived`). The table the view resolves at
  read time. It holds unresolved, event-time rows, so query the view instead.
- `snowplow_identities_identities` (`derived`). The renamed `new_identities`.
- `snowplow_identities_stg_identity_events` (`scratch`). Identity context events read from
  `atomic.events`, replacing the old base layer.

Every derived table also gains a `load_tstamp` column, which is how each model now tracks
its own progress. See [Each model now tracks its own progress](#each-model-now-tracks-its-own-progress).

### Each model now tracks its own progress

The shared manifest table and the hooks that maintained it are gone. Each model asks its
own table for the newest `load_tstamp` it holds and processes forward from there, so a
failed run leaves that model slightly behind and it catches up on the next one. This
deletes the manifest, three supporting models, the whole `_this_run` scratch layer, and
every run hook. 15 models become 5 plus a view.

Config that no longer exists: `snowplow__allow_refresh`, `snowplow__dev_target_name`,
and the `models_to_remove` var (all belonged to the manifest). A `--full-refresh` now
simply rebuilds from `snowplow__start_date`, with no manifest to guard.

Config that is new:

| Var | Default | What it does |
| --- | --- | --- |
| `snowplow__lookback_days` | 1 | Days of overlap re-scanned each run for late-arriving data. Also the run-twice idempotency window. |
| `snowplow__partition_tstamp` | `load_tstamp` | The column `atomic.events` is physically partitioned by. Set to `collector_tstamp` if that is your partition key, so staging scans still prune. |
| `snowplow__partition_tstamp_type` | `timestamp` | `timestamp` or `date`, matching the above column's type. |
| `snowplow__partition_buffer_days` | 2 | Extra days on the prune predicate to absorb collector-to-load lag. |

### Tables you must drop before the first run

`snowplow_id_mapping` and `merge_events` keep their names, grain and values, and every
column they had before. But both change shape in ways dbt cannot apply to a table that
already exists, so upgrading in place does not work:

- `load_tstamp` is a new column on both. `on_schema_change: append_new_columns` adds it to
  the existing rows as `NULL`, and both models now test `load_tstamp` `not_null`. Every
  pre-upgrade row would fail that test until the backfill in step 3 overwrote it.
- The partition column changes to `load_tstamp`, from `merged_at` and `derived_tstamp`, because
  that is the column each model now watermarks and filters on. dbt does not repartition an
  existing incremental table. Depending on the adapter you either keep the old partitioning
  and lose pruning on the column the model now scans, or the run fails on the mismatch.

So drop both and let dbt rebuild them:

```sql
drop table if exists <derived_schema>.snowplow_identities_snowplow_id_mapping;
drop table if exists <derived_schema>.snowplow_identities_merge_events;
```

Nothing is lost. Both are rebuilt from `atomic.events` by the backfill in step 3, which
replays from `snowplow__start_date` anyway.

`identifier_mapping` needs no manual step, because dbt replaces the old table with the view.
`new_identities` is superseded by `identities`, so the old table is left behind for you to
drop in step 4.

### Upgrade steps

1. **Bump the package version** in your `packages.yml`.

2. **Drop `snowplow_id_mapping` and `merge_events`**, per
   [Tables you must drop before the first run](#tables-you-must-drop-before-the-first-run).

3. **Expect one backfill.** Every derived model now carries a `load_tstamp` column that
   did not exist before, so on the first run each model's watermark resolves to
   `snowplow__start_date` and history is replayed from there, advancing
   `snowplow__backfill_limit_days` per run. With the default 30-day limit and a start
   date a year back, that is about 12 runs. To compress it, raise the limit temporarily:

   ```bash
   dbt run --vars '{snowplow__backfill_limit_days: 400}'
   ```

   Each run logs exactly what it is processing, so you can watch progress:

   ```
   Snowplow: snowplow_identities_identities at watermark 2026-07-14 -- processing load_tstamp from 2026-07-14 to 2026-08-14
   ```

   Until the backfill completes, the tables only cover the period processed so far.
   Nothing is lost. Every row is rebuilt from `atomic.events`.

4. **Drop the old tables** once you are happy, since dbt no longer manages them:

   ```sql
   -- removed outputs
   drop table if exists <derived_schema>.snowplow_identities_id_changes;
   drop table if exists <derived_schema>.snowplow_identities_id_mapping_scd;
   drop table if exists <derived_schema>.snowplow_identities_new_identities;
   drop table if exists <derived_schema>.snowplow_identities_snowplow_id_mapping_affected;

   -- the retired manifest framework
   drop table if exists <manifest_schema>.snowplow_identities_incremental_manifest;
   drop table if exists <scratch_schema>.snowplow_identities_base_events_this_run;
   drop table if exists <scratch_schema>.snowplow_identities_base_new_event_limits;
   drop table if exists <scratch_schema>.snowplow_identities_merge_events_this_run;
   drop table if exists <scratch_schema>.snowplow_identities_new_identities_this_run;
   drop table if exists <scratch_schema>.snowplow_identities_new_identifiers_this_run;
   drop table if exists <scratch_schema>.snowplow_identities_identifier_mapping_this_run;
   drop table if exists <scratch_schema>.snowplow_identities_snowplow_id_mapping_this_run;
   drop table if exists <scratch_schema>.snowplow_identities_id_changes_this_run;
   ```

   dbt replaces the old `identifier_mapping` table with the view automatically, so that
   one needs no manual step.

### Sparse event streams no longer stall a model

A model's window can only advance if it finds data in it. Left alone, a gap between events
longer than `snowplow__backfill_limit_days` would strand a model. The window comes back empty,
the watermark does not move, and the next run recomputes the same window. This mattered most
for `merge_events` and `snowplow_id_mapping`, which advance off `identity_merge` events only.
Those are far sparser than identity events, and often more than a day apart.

Identity context events are dense, so `snowplow_identities_stg_identity_events` always has
current data. Every other model uses its watermark as an anchor:

- The lower bound is always the model's own watermark, so a model that errors re-scans its
  last window rather than skipping it. The manifest gave you that by taking the minimum across
  models.
- The upper bound is whichever is further ahead, its own watermark plus
  `snowplow__backfill_limit_days`, or the anchor's watermark. A merge landing anywhere after
  the model's watermark is inside the window.

Two things follow. `snowplow__backfill_limit_days` becomes a floor rather than a cap for a
model sitting behind the anchor, though it still paces the anchor itself, so backfills stay
throttled. And between merges `merge_events` scans a widening range, from your last merge up
to where staging has reached. That scan is pruned on `load_tstamp` and `event_name`, which
`atomic.events` is usually partitioned or clustered on, and it collapses as soon as a merge
lands.

One case this does not cover. If `snowplow__start_date` is well before your data begins, the
anchor's own first window is empty and it stalls too. The manifest framework did the same, so
this is not new, but set `snowplow__start_date` near the start of your data.

### Run-window logging

Every model logs its watermark and the window it is about to process, replacing the old
`print_run_limits` output. In normal operation all five models report the same window; during
a backfill, or where one model is catching up behind the anchor, they legitimately differ.

---

## Why identifier_mapping became a view

Previously, a merge physically rewrote identifier rows to point at the winning
identity, then deleted the leftovers via a post-hook. That approach caused a recurring
class of bug: stale rows after cascading merges, duplicate keys when one identifier was
shared across identities, and a `MERGE` failure when two re-pointed rows collapsed onto one
key.

Now `identifier_mapping_base` stores one row per `(snowplow_id, id_type, id_value)`
under an immutable key that never changes, and the view resolves each row to its
current `active_snowplow_id` on read. Merges are reflected the moment
`snowplow_id_mapping` updates, and nothing upstream is ever rewritten.

Two consequences worth knowing:

- **The view does the resolution work on every query.** If you query it heavily and it
  becomes a bottleneck, materialize it in your own project:

  ```sql
  -- models/my_identifier_mapping.sql
  {{ config(materialized='table') }}
  select * from {{ ref('snowplow_identities_identifier_mapping') }}
  ```

- **An identifier can now belong to more than one identity over its lifetime.** If an
  identifier expires from the identity engine and later reappears under a new identity,
  both rows are kept in `identifier_mapping_base`. Previously the second sighting
  overwrote the first. The view still shows one row per
  `(active_snowplow_id, id_type, id_value)`, so this only surfaces if the two identities
  have not merged, in which case you correctly see two owners.

---

## Rebuilding the wide identity row

`identities` no longer carries `domain_userid`, `user_id`, and friends. Those values
live in `identifier_mapping`, which keeps *every* value an identity has carried rather
than collapsing to one per type. If you want the old wide shape, pivot the mapping back:

```sql
-- models/my_identities_wide.sql
with identifiers as (
    select
        active_snowplow_id,
        max(case when id_type = 'DOMAIN_USERID' then id_value end) as domain_userid,
        max(case when id_type = 'USER_ID' then id_value end) as user_id
    from {{ ref('snowplow_identities_identifier_mapping') }}
    group by 1
)

select
    i.*,
    x.domain_userid,
    x.user_id
from {{ ref('snowplow_identities_identities') }} i
left join {{ ref('snowplow_identities_snowplow_id_mapping') }} m
    on i.snowplow_id = m.snowplow_id
left join identifiers x
    on coalesce(m.active_snowplow_id, i.snowplow_id) = x.active_snowplow_id
```

The `max()` matters. Where an identity carries several values of one type it picks one
arbitrarily, which is the lossiness that motivated removing these columns. Join
`identifier_mapping` at the identifier grain instead where you can.

---

## Rebuilding id_changes

An append-only log of every identity creation and merge. Column names below match the
removed table exactly, so a drop-in replacement only needs the `ref()` updated.

`effective_at` is event time (when the change happened); `changed_at` is processing
time (when dbt saw it).

```sql
-- models/my_id_changes.sql
{{ config(
    materialized='incremental',
    unique_key='id_change_key',
    on_schema_change='append_new_columns'
) }}

with created as (
    select
        snowplow_id,
        cast(null as {{ dbt.type_string() }}) as previous_snowplow_id,
        first_derived_tstamp as effective_at,
        'created' as change_type,
        first_seen_event_id,
        first_app_id as first_seen_app_id
    from {{ ref('snowplow_identities_identities') }}
    {% if is_incremental() %}
      where first_derived_tstamp > (
          select coalesce(max(effective_at), '1970-01-01')
          from {{ this }} where change_type = 'created'
      )
    {% endif %}
),

-- One row per (parent, child) edge in every merge event. Snowflake syntax; on
-- BigQuery use `from ... as p, unnest(p.merged) as m` and drop the `:field::type`
-- casts. Keep the flatten in its own CTE: combining a lateral flatten and a join in
-- one FROM clause makes Snowflake fail with an internal optimizer error.
merged_flat as (
    select
        p.active_snowplow_id,
        m.value:snowplow_id::{{ dbt.type_string() }} as previous_snowplow_id,
        m.value:merged_at::{{ dbt.type_timestamp() }} as effective_at,
        m.value:triggering_event_id::{{ dbt.type_string() }} as first_seen_event_id
    from {{ ref('snowplow_identities_merge_events') }} as p,
        table(flatten(input => p.merged)) as m
),

-- Merge events carry the full cumulative list of merged ids, so the same edge
-- reappears in every later merge event for its group. Keep the earliest sighting of
-- each edge -- that is when the change actually happened. This scans the whole merge
-- log rather than just the new batch, deliberately: the earliest sighting can only be
-- found globally, and computing it globally makes the result identical on every run,
-- so the incremental merge below is a no-op for edges already stored. The merge log
-- is small relative to atomic.events, so the full scan is cheap.
merged as (
    select
        f.active_snowplow_id as snowplow_id,
        f.previous_snowplow_id,
        f.effective_at,
        'merged' as change_type,
        f.first_seen_event_id,
        i.first_app_id as first_seen_app_id
    from merged_flat f
    left join {{ ref('snowplow_identities_identities') }} i
        on f.previous_snowplow_id = i.snowplow_id
    qualify row_number() over (
        partition by f.active_snowplow_id, f.previous_snowplow_id
        order by f.effective_at asc
    ) = 1
),

combined as (
    select * from created
    union all
    select * from merged
)

select
    {{ dbt_utils.generate_surrogate_key(['snowplow_id', 'previous_snowplow_id']) }} as id_change_key,
    snowplow_id,
    previous_snowplow_id,
    effective_at,
    {{ snowplow_utils.current_timestamp_in_utc() }} as changed_at,
    change_type,
    first_seen_event_id,
    first_seen_app_id
from combined
```

A child legitimately appears under more than one parent when merges cascade, first under an
intermediate and later under the final root. Both edges are real changes, so both get a row.
That is what makes this a change log rather than a current-state mapping. For current state,
use `snowplow_id_mapping`.

### Verified against the removed model

Run against the package's integration fixtures, this recipe produces the same 150 rows as
the deleted table, with the same `snowplow_id`, `previous_snowplow_id`, `change_type`,
`first_seen_event_id` and `first_seen_app_id`. Without the `qualify`, the cumulative arrays
inflate `merged` from 63 rows to 104.

One known difference. On 9 of the 63 `merged` rows, all of them cascading merges,
`effective_at` differs. This recipe uses the engine's own `merged_at`, so a child re-rooted by
a later merge keeps the timestamp of when it was first merged. The removed model stamped it
with the time of the later merge instead, using an internal snapshot table that no longer
exists. They answer different questions, "when was this child merged" against "when did it
start pointing at this root". If you need the second reading, `snowplow_id_mapping` already
has current state, and reproducing the history exactly means tracking root changes run over
run.

---

## Rebuilding id_mapping_scd

A Type 2 history answering "which identity did this id belong to as of *then*". Build it
on top of `my_id_changes` above.

```sql
-- models/my_id_mapping_scd.sql
{{ config(materialized='table') }}

with changes as (
    select
        -- the id being tracked: the child for merges, the new id for creates
        case when change_type = 'merged' then previous_snowplow_id else snowplow_id end as snowplow_id,
        snowplow_id as active_snowplow_id,
        effective_at,
        changed_at,
        change_type,
        first_seen_event_id,
        first_seen_app_id
    from {{ ref('my_id_changes') }}
),

-- Keep the earliest record per (snowplow_id, active_snowplow_id) so re-emitted
-- cumulative merges do not create duplicate validity periods.
deduped as (
    select *
    from changes
    qualify row_number() over (
        partition by snowplow_id, active_snowplow_id
        order by effective_at asc
    ) = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['snowplow_id', 'effective_at', 'change_type']) }} as scd_key,
    snowplow_id,
    active_snowplow_id,
    effective_at,
    lead(effective_at) over (partition by snowplow_id order by effective_at asc) as superseded_at,
    changed_at,
    change_type,
    first_seen_event_id,
    first_seen_app_id,
    lead(effective_at) over (partition by snowplow_id order by effective_at asc) is null as is_current
from deduped
```

Point-in-time query:

```sql
select s.snowplow_id, scd.active_snowplow_id
from sessions s
join my_id_mapping_scd scd
    on s.snowplow_id = scd.snowplow_id
where scd.effective_at <= :as_of_timestamp
    and (scd.superseded_at is null or scd.superseded_at > :as_of_timestamp)
```

Materialized as a table rather than incrementally, because `superseded_at` and
`is_current` on existing rows change when a new merge arrives. If that gets expensive,
make it incremental on `scd_key` and restrict `changes` to `snowplow_id`s touched in the
current batch. That is what the removed model did, and it was the most complex model in the
package.
