{# The MERGE unique_key (uuid) includes active_snowplow_id, so repointed rows
   get new UUIDs and old rows survive. This post_hook cleans them up.
   Note: refs snowplow_id_mapping_this_run which must be materialized as a
   table (not ephemeral), otherwise the post_hook cannot query it. #}
{% macro delete_stale_identifier_mapping_rows() %}
    {% if is_incremental() %}
        delete from {{ this }}
        where active_snowplow_id in (
            select snowplow_id
            from {{ ref('snowplow_identities_snowplow_id_mapping_this_run') }}
        )
    {% endif %}
{% endmacro %}
