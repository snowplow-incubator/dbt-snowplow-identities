{% macro _get_cluster_by_values(model) %}
    {{ return(adapter.dispatch('_get_cluster_by_values', '<YOUR PACKAGE NAME>')(model)) }}
{% endmacro %}


{% macro default___get_cluster_by_values(model) %}
    {% if model == 'lifecycle_manifest' %}
        {{ return(snowplow_utils.get_value_by_target_type(bigquery_val=["session_identifier"], snowflake_val=["to_date(start_tstamp)"])) }}
    {% else %}
        {{ exceptions.raise_compiler_error(
      "Snowplow Error: Model "~model~" not defined for cluster by."
      ) }}
    {% endif %}
{% endmacro %}
