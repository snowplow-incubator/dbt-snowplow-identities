{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro get_cluster_by_values(model) %}
    {{ return(adapter.dispatch('get_cluster_by_values', 'snowplow_identities')(model)) }}
{% endmacro %}


{% macro default__get_cluster_by_values(model) %}
    {% if model == 'merge_events' %}
        {{ return(snowplow_utils.get_value_by_target_type(bigquery_val=["active_snowplow_id"], snowflake_val=["active_snowplow_id"])) }}
    {% elif model == 'new_identities' %}
        {{ return(snowplow_utils.get_value_by_target_type(bigquery_val=["snowplow_id"], snowflake_val=["snowplow_id"])) }}
    {% elif model == 'identifier_mapping' %}
        {{ return(snowplow_utils.get_value_by_target_type(bigquery_val=["active_snowplow_id", "uuid"], snowflake_val=["active_snowplow_id", "uuid"])) }}
    {% elif model == 'id_changes' %}
        {{ return(snowplow_utils.get_value_by_target_type(bigquery_val=["snowplow_id", "previous_snowplow_id"], snowflake_val=["snowplow_id", "previous_snowplow_id"])) }}
    {% elif model == 'snowplow_id_mapping' %}
        {{ return(snowplow_utils.get_value_by_target_type(bigquery_val=["active_snowplow_id", "snowplow_id"], snowflake_val=["active_snowplow_id", "snowplow_id"])) }}
    {% else %}
        {{ exceptions.raise_compiler_error(
      "Snowplow Error: Model "~model~" not defined for cluster by."
      ) }}
    {% endif %}
{% endmacro %}
