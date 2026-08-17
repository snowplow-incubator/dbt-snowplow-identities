{# Companion DELETE for the merge limit collapse stage in identifier_mapping. The
   MERGE rewrites rows under the picked snowplow_id but cannot remove the old rows,
   which have different uuids. This runs after the MERGE and deletes them. It repeats
   the model's pick over the table as it now stands: per identifier the snowplow_id
   with the earliest created_at in new_identities wins, and identifiers with any
   undated snowplow_id are skipped. A row still keyed to an already-merged
   snowplow_id is read through snowplow_id_mapping first, the same way the model
   reads it. Only rows whose identifier is in this batch are deleted; anything else
   waits until its identifier next appears. Everything is filtered to this batch's
   identifiers before ranking, so cost follows the batch, and a run with no new
   events deletes nothing. #}
{% macro merge_limit_collapse_delete_stale_rows() %}
    {% if is_incremental() and var('snowplow__merge_limit_collapse', false) %}
        delete from {{ this }}
        where uuid in (
            with run_identifiers as (
                select distinct id_type, id_value
                from {{ ref('snowplow_identities_identifier_mapping_this_run') }}
                where {{ snowplow_utils.is_run_with_new_events('snowplow_identities') }}
            )

            -- a delete is only possible when an identifier has rows under more
            -- than one stored snowplow_id, so everything downstream is limited
            -- to those identifiers
            , multi_identifiers as (
                select t.id_type, t.id_value
                from {{ this }} t
                inner join run_identifiers n
                    on n.id_type = t.id_type and n.id_value = t.id_value
                group by t.id_type, t.id_value
                having count(distinct t.active_snowplow_id) > 1
            )

            -- rows for those identifiers, read through snowplow_id_mapping so a
            -- row still keyed to an already-merged snowplow_id counts under its
            -- current one
            , run_rows as (
                select
                    t.uuid,
                    t.id_type,
                    t.id_value,
                    t.active_snowplow_id as stored_snowplow_id,
                    coalesce(sm.active_snowplow_id, t.active_snowplow_id) as active_snowplow_id
                from {{ this }} t
                inner join multi_identifiers n
                    on n.id_type = t.id_type and n.id_value = t.id_value
                left join {{ ref('snowplow_identities_snowplow_id_mapping') }} sm
                    on t.active_snowplow_id = sm.snowplow_id
            )

            -- one row per snowplow_id, mirroring the model's mlc_identity_ages
            , identity_ages as (
                select snowplow_id, min(created_at) as created_at
                from {{ ref('snowplow_identities_new_identities') }}
                group by snowplow_id
            )

            -- same rule as the model's mlc_exempt_identifiers: if any snowplow_id
            -- with a row for an identifier has no created_at, nothing for that
            -- identifier is deleted this run
            , exempt_identifiers as (
                select distinct r.id_type, r.id_value
                from run_rows r
                left join identity_ages ni
                    on r.active_snowplow_id = ni.snowplow_id
                where ni.created_at is null
            )

            , pick as (
                select id_type, id_value, active_snowplow_id as pick_snowplow_id
                from (
                    select
                        r.id_type,
                        r.id_value,
                        r.active_snowplow_id,
                        row_number() over (partition by r.id_type, r.id_value order by ni.created_at asc nulls last, r.active_snowplow_id asc) as rn
                    from run_rows r
                    left join identity_ages ni
                        on r.active_snowplow_id = ni.snowplow_id
                ) ranked
                where rn = 1
            )

            -- Compare the id AS STORED in the row, not the id after reading
            -- through snowplow_id_mapping. A leftover row keyed to a
            -- merged-away snowplow_id reads as the chosen id and would never
            -- delete; by its stored id it differs from the chosen id and goes.
            select r.uuid
            from run_rows r
            inner join pick p
                on p.id_type = r.id_type and p.id_value = r.id_value
            left join exempt_identifiers ex
                on ex.id_type = r.id_type and ex.id_value = r.id_value
            where r.stored_snowplow_id != p.pick_snowplow_id
            and ex.id_type is null
        )
    {% endif %}
{% endmacro %}
