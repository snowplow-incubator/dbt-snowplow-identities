{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{ config(
    materialized="incremental",
    on_schema_change="append_new_columns",
    unique_key="uuid",
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    partition_by=snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "last_seen_at",
      "data_type": "timestamp"
    }, databricks_val='last_seen_at_date'),
    cluster_by=snowplow_identities.get_cluster_by_values('identifier_mapping'),
    tags=["derived"],
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    meta={'upsert_date_key': 'last_seen_at', 'snowplow_optimize': true},
    post_hook=["{{ snowplow_identities.delete_stale_identifier_mapping_rows() }}", "{{ snowplow_identities.merge_limit_collapse_delete_stale_rows() }}"]
) }}

with new_from_this_run as (
    select *
    from {{ ref('snowplow_identities_identifier_mapping_this_run') }}
    where {{ snowplow_utils.is_run_with_new_events('snowplow_identities') }}
)

, merged_ids_this_run as (
    select distinct snowplow_id
    from {{ ref('snowplow_identities_snowplow_id_mapping_this_run') }}
)

, id_mapping as (
    select snowplow_id, active_snowplow_id
    from {{ ref('snowplow_identities_snowplow_id_mapping') }}
)

{% if is_incremental() %}

, existing_to_repoint as (
    select
        {{ dbt_utils.generate_surrogate_key(['coalesce(id_map.active_snowplow_id, hist.active_snowplow_id)', 'hist.id_type', 'hist.id_value']) }} as uuid,
        coalesce(id_map.active_snowplow_id, hist.active_snowplow_id) as active_snowplow_id,
        hist.id_type,
        hist.id_value,
        hist.first_app_id,
        hist.last_app_id,
        hist.first_seen_at,
        hist.last_seen_at,
        hist.first_seen_event_id
    from {{ this }} hist
    left join id_mapping id_map
        on hist.active_snowplow_id = id_map.snowplow_id
    where hist.active_snowplow_id in (select snowplow_id from merged_ids_this_run)
    and not exists (
        select 1 from new_from_this_run n
        where n.active_snowplow_id = coalesce(id_map.active_snowplow_id, hist.active_snowplow_id)
        and n.id_type = hist.id_type
        and n.id_value = hist.id_value
    )
)

-- Resolve historical rows' active_snowplow_id through the current mapping,
-- so rows with a stale parent (pre-merge) can match against the new parent.
-- Filtered to only identifiers present in this batch to avoid full table scan.
, historical_resolved as (
    select
        coalesce(im.active_snowplow_id, h.active_snowplow_id) as resolved_active_snowplow_id,
        h.id_type,
        h.id_value,
        h.first_app_id,
        h.last_app_id,
        h.first_seen_at,
        h.last_seen_at,
        h.first_seen_event_id
    from {{ this }} h
    inner join (select distinct id_type, id_value from new_from_this_run) n
        on h.id_type = n.id_type and h.id_value = n.id_value
    left join id_mapping im
        on h.active_snowplow_id = im.snowplow_id
    where h.active_snowplow_id in (
        select distinct active_snowplow_id from new_from_this_run
        union all
        select distinct snowplow_id from merged_ids_this_run
    )
)

, new_with_history as (
    select
        c.uuid,
        c.active_snowplow_id,
        c.id_type,
        c.id_value,
        case when h.first_seen_at is not null and h.first_seen_at <= c.first_seen_at
             then h.first_app_id else c.first_app_id end as first_app_id,
        case when h.last_seen_at is not null and h.last_seen_at >= c.last_seen_at
             then h.last_app_id else c.last_app_id end as last_app_id,
        least(c.first_seen_at, coalesce(h.first_seen_at, c.first_seen_at)) as first_seen_at,
        greatest(c.last_seen_at, coalesce(h.last_seen_at, c.last_seen_at)) as last_seen_at,
        case when h.first_seen_at is not null and h.first_seen_at <= c.first_seen_at
             then h.first_seen_event_id else c.first_seen_event_id end as first_seen_event_id
    from new_from_this_run c
    left join historical_resolved h
        on c.active_snowplow_id = h.resolved_active_snowplow_id
        and c.id_type = h.id_type
        and c.id_value = h.id_value
)

, new_ranked as (
    select
        uuid,
        active_snowplow_id,
        id_type,
        id_value,
        {{ snowplow_identities.identifier_mapping_ranked_columns() }}
    from new_with_history
)

, new_aggregated as (
    select
        uuid,
        active_snowplow_id,
        id_type,
        id_value,
        first_app_id,
        last_app_id,
        first_seen_at,
        last_seen_at,
        first_seen_event_id
    from new_ranked
    where rn = 1
)

-- Dedupe re-pointed rows so multiple pre-merge parents resolving to one active id don't emit duplicate uuids.
, existing_ranked as (
    select
        uuid,
        active_snowplow_id,
        id_type,
        id_value,
        {{ snowplow_identities.identifier_mapping_ranked_columns() }}
    from existing_to_repoint
)

, existing_aggregated as (
    select
        uuid,
        active_snowplow_id,
        id_type,
        id_value,
        first_app_id,
        last_app_id,
        first_seen_at,
        last_seen_at,
        first_seen_event_id
    from existing_ranked
    where rn = 1
)

, combined as (
    select * from new_aggregated
    union all
    select * from existing_aggregated
)

{% else %}

, combined as (
    select * from new_from_this_run
)

{% endif %}

{% if not var('snowplow__merge_limit_collapse', false) %}

select * from combined

{% else %}

-- Merge limit collapse. The identity service stops merging an identity at its
-- merge limit and links instead. A link makes the linked events carry the
-- snowplow_id of the identity they linked into, but no merge event is emitted.
-- Without a merge event nothing deletes the identifier's old row here, so the
-- identifier keeps two rows with two different active_snowplow_ids. The stage
-- below repairs that. The service always links into the identity with the
-- earliest created_at, so per identifier the snowplow_id with the earliest
-- created_at in new_identities is the one the service now resolves to. That
-- snowplow_id must also be the one the identifier was last seen under, which
-- separates a merge-limit link from a TTL eviction: after a link the oldest
-- identity keeps receiving the identifier's events, after an eviction the
-- identifier moves to a new identity and the oldest one is abandoned. When both
-- conditions hold, every row for the identifier is rewritten under that
-- snowplow_id and the old rows are deleted afterwards. ONLY correct when no
-- identifier is configured as unique in the identity service: uniqueness is the
-- one way two separate identities can share an identifier, and this stage would
-- merge them.

, mlc_rows as (
    select
        active_snowplow_id,
        active_snowplow_id as raw_snowplow_id,
        id_type, id_value, first_app_id, last_app_id, first_seen_at, last_seen_at, first_seen_event_id,
        true as from_this_run,
        false as resolved_changed
    from combined

    {% if is_incremental() %}
    union all
    select
        coalesce(im.active_snowplow_id, h.active_snowplow_id) as active_snowplow_id,
        h.active_snowplow_id as raw_snowplow_id,
        h.id_type,
        h.id_value,
        h.first_app_id,
        h.last_app_id,
        h.first_seen_at,
        h.last_seen_at,
        h.first_seen_event_id,
        false as from_this_run,
        im.active_snowplow_id is not null and im.active_snowplow_id != h.active_snowplow_id as resolved_changed
    from {{ this }} h
    inner join (select distinct id_type, id_value from new_from_this_run) ids
        on h.id_type = ids.id_type and h.id_value = ids.id_value
    left join id_mapping im
        on h.active_snowplow_id = im.snowplow_id
    {% endif %}
)

-- One pass per identifier computes the four things the stage needs.
-- multi: does the identifier have rows under more than one snowplow_id? The
--   check uses the id as stored, before reading through snowplow_id_mapping,
--   so a leftover row keyed to an already-merged snowplow_id still counts.
-- skipped: does any of its snowplow_ids have no created_at in new_identities?
--   Without a date we cannot know which identity is oldest, so nothing for
--   that identifier is rewritten or deleted this run.
-- pick_snowplow_id: the snowplow_id with the earliest created_at. The same
--   ordering applies to every identifier, so rows only ever move from a
--   younger snowplow_id to an older one. Ties fall back to active_snowplow_id
--   so the pick is always deterministic.
-- last_seen_snowplow_id: the snowplow_id holding the identifier's latest
--   last_seen_at. Rows only move when it equals pick_snowplow_id, which is
--   what separates a link from an eviction, as described above.
-- One row per snowplow_id, whatever state new_identities is in. Without this,
-- a duplicate row upstream would fan out through the join below and put two
-- rows with the same uuid into the MERGE source, which halts the run.
, mlc_identity_ages as (
    select snowplow_id, min(created_at) as created_at
    from {{ ref('snowplow_identities_new_identities') }}
    group by snowplow_id
)

, mlc_flagged as (
    select
        r.*,
        min(r.raw_snowplow_id) over (partition by r.id_type, r.id_value)
            != max(r.raw_snowplow_id) over (partition by r.id_type, r.id_value) as multi,
        max(case when ni.created_at is null then 1 else 0 end) over (partition by r.id_type, r.id_value) = 1 as skipped,
        first_value(r.active_snowplow_id) over (
            partition by r.id_type, r.id_value
            order by ni.created_at asc nulls last, r.active_snowplow_id asc
            rows between unbounded preceding and current row
        ) as pick_snowplow_id,
        first_value(r.active_snowplow_id) over (
            partition by r.id_type, r.id_value
            order by r.last_seen_at desc, ni.created_at asc nulls last, r.active_snowplow_id asc
            rows between unbounded preceding and current row
        ) as last_seen_snowplow_id
    from mlc_rows r
    left join mlc_identity_ages ni
        on r.active_snowplow_id = ni.snowplow_id
)

-- Move rows of the identifiers that need it. A skipped identifier's rows keep
-- the id AS STORED, and the read through snowplow_id_mapping does not count
-- as a change for them; otherwise a leftover row from an old merge would come
-- out rewritten and emitted for an identifier we promised to leave alone.
, mlc_repointed as (
    select
        case when skipped or pick_snowplow_id != last_seen_snowplow_id then raw_snowplow_id
             else pick_snowplow_id end as active_snowplow_id,
        id_type,
        id_value,
        first_app_id,
        last_app_id,
        first_seen_at,
        last_seen_at,
        first_seen_event_id,
        from_this_run
            or (not skipped and pick_snowplow_id = last_seen_snowplow_id
                and (resolved_changed or active_snowplow_id != pick_snowplow_id)) as changed
    from mlc_flagged
    where multi
)

-- Emit a row only when something about it changed this run: it came from this
-- batch, it was moved under the pick, or its old snowplow_id now maps to a new
-- one in snowplow_id_mapping. Rewriting unchanged table rows would grow the
-- MERGE source and widen the last_seen_at window the MERGE scans.
, mlc_ranked as (
    select
        {{ dbt_utils.generate_surrogate_key(['active_snowplow_id', 'id_type', 'id_value']) }} as uuid,
        active_snowplow_id,
        id_type,
        id_value,
        {{ snowplow_identities.identifier_mapping_ranked_columns() }},
        max(case when changed then 1 else 0 end) over (partition by active_snowplow_id, id_type, id_value) as partition_changed
    from mlc_repointed
)

select
    uuid,
    active_snowplow_id,
    id_type,
    id_value,
    first_app_id,
    last_app_id,
    first_seen_at,
    last_seen_at,
    first_seen_event_id
from mlc_ranked
where rn = 1
and partition_changed = 1

union all

-- Identifiers with a single snowplow_id need none of the machinery above:
-- their batch rows pass straight through, and their unchanged table rows are
-- not re-emitted.
select
    {{ dbt_utils.generate_surrogate_key(['active_snowplow_id', 'id_type', 'id_value']) }} as uuid,
    active_snowplow_id,
    id_type,
    id_value,
    first_app_id,
    last_app_id,
    first_seen_at,
    last_seen_at,
    first_seen_event_id
from mlc_flagged
where not multi
and from_this_run

{% endif %}
