# Migration guide

## Upgrading to 0.2.0

This release removes two output tables, renames a third, and changes how
`snowplow_identities_identifier_mapping` is built. **The table most people query —
`snowplow_identities_identifier_mapping` — keeps its name, columns, and grain, so most
consumers need no changes at all.**

### What changed

| Table | Change | Action needed |
| --- | --- | --- |
| `snowplow_identities_snowplow_id_mapping` | None | None |
| `snowplow_identities_merge_events` | None | None |
| `snowplow_identities_identifier_mapping` | Now a **view** over a new table. Same columns, same grain, same values. | None for consumers |
| `snowplow_identities_new_identities` | Renamed to `snowplow_identities_identities`; per-identifier columns removed | Update references; see [Rebuilding the wide identity row](#rebuilding-the-wide-identity-row) |
| `snowplow_identities_id_changes` | **Removed** | See [Rebuilding id_changes](#rebuilding-id_changes) |
| `snowplow_identities_id_mapping_scd` | **Removed** | See [Rebuilding id_mapping_scd](#rebuilding-id_mapping_scd) |

New models you will see appear, both in the `derived` schema:

- `snowplow_identities_identifier_mapping_base` — the durable table behind the view.
  You should not need to query it; it holds unresolved, event-time rows.
- `snowplow_identities_identities` — the renamed `new_identities`.

### Upgrade steps

1. **Bump the package version** in your `packages.yml`.

2. **Clear the manifest rows for the removed models.** On your first run, pass:

   ```bash
   dbt run --vars '{models_to_remove: [snowplow_identities_id_changes, snowplow_identities_id_mapping_scd, snowplow_identities_new_identities, snowplow_identities_new_identities_this_run]}'
   ```

   Stale rows are harmless if you skip this, but leaving them behind makes the manifest
   misleading when you come to debug a run.

3. **Expect a backfill.** `identifier_mapping_base` and `identities` are new models, so
   the incremental framework replays from `snowplow__start_date` to populate them,
   advancing `snowplow__backfill_limit_days` per run (you will see
   `Snowplow: New Snowplow incremental model. Backfilling` in the logs). With the
   default 30-day limit and a start date a year back, that is about 12 runs. To
   compress it, raise the limit temporarily:

   ```bash
   dbt run --vars '{snowplow__backfill_limit_days: 400}'
   ```

   Until the backfill completes, `identifier_mapping` will only cover the period
   processed so far. Nothing is lost — every row is rebuilt from `atomic.events`.

4. **Drop the old tables** once you are happy, since dbt no longer manages them:

   ```sql
   drop table if exists <your_derived_schema>.snowplow_identities_id_changes;
   drop table if exists <your_derived_schema>.snowplow_identities_id_mapping_scd;
   drop table if exists <your_derived_schema>.snowplow_identities_new_identities;
   drop table if exists <your_derived_schema>.snowplow_identities_snowplow_id_mapping_affected;
   ```

   dbt replaces the old `identifier_mapping` table with the view automatically, so
   that one needs no manual step.

---

## Why identifier_mapping became a view

Previously, a merge physically rewrote identifier rows to point at the winning
identity, then deleted the leftovers via a post-hook. That approach caused a recurring
class of bug — stale rows after cascading merges, duplicate keys when one identifier
was shared across identities, and a `MERGE` failure when two re-pointed rows collapsed
onto one key.

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
  have not merged — in which case you will correctly see two owners.

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

Note the `max()`: where an identity carries several values of one type, this picks one
arbitrarily, which is exactly the lossiness that motivated removing these columns.
Prefer joining `identifier_mapping` directly at the identifier grain if you can.

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

A child legitimately appears under more than one parent when merges cascade — first
under an intermediate, later under the final root. Both edges are real changes, so both
get a row; that is what makes this a change *log* rather than a current-state mapping.
For current state, use `snowplow_id_mapping`.

### Verified against the removed model

Run against the package's integration fixtures, this recipe produces the same 150 rows
as the deleted table — the same 87 `created` and 63 `merged` edges, with the same
`snowplow_id`, `previous_snowplow_id`, `change_type`, `first_seen_event_id` and
`first_seen_app_id` throughout. Without the `qualify`, the cumulative arrays inflate
`merged` from 63 to 104.

**One known difference.** On 9 of the 63 `merged` rows — all of them cascading merges —
`effective_at` differs. This recipe uses the engine's own `merged_at` for the edge, so a
child re-rooted by a later merge keeps the timestamp of when it was originally merged.
The removed model instead stamped such an edge with the time of the *later* merge that
re-rooted it, a value it reconstructed from an internal pre-run snapshot table that no
longer exists. Neither is wrong, they answer different questions: "when was this child
merged" versus "when did it start pointing at this particular root".

If you need the second reading, note that no simple rule over the merge log reproduces
it: taking the earliest or latest `merged_at` per edge each leaves 9 rows different, the
merge event's own `derived_tstamp` leaves 63, and the top-level merge time of the first
event listing the edge leaves 6. Reconstructing it exactly means tracking root changes
run over run, which is the complexity the removal was meant to shed. For current state
rather than history, `snowplow_id_mapping` already has the answer.

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
current batch — that is what the removed model did, and it was the most complex model in
the package.
