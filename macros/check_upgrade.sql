{# Fail before changing any outputs when a 0.1.0 installation needs rebuilding. #}
{% macro check_upgrade() %}
  {# Register hook dependencies during parsing, before selected_resources is populated. #}
  {% set expected = [ref('snowplow_identities_merge_events'), ref('snowplow_identities_snowplow_id_mapping')] %}
  {% if execute and flags.WHICH in ['run', 'build'] %}
    {% set package_models = graph.nodes.values()
      | selectattr('resource_type', 'equalto', 'model')
      | selectattr('package_name', 'equalto', 'snowplow_identities') | list %}
    {% set selected_models = package_models | selectattr('unique_id', 'in', selected_resources) | list %}
    {% if not selected_models %}{{ return('') }}{% endif %}

    {% set legacy_tables = [] %}
    {% for relation in expected %}
      {% set existing = adapter.get_relation(database=relation.database, schema=relation.schema, identifier=relation.identifier) %}
      {% if existing is not none %}
        {% set names = adapter.get_columns_in_relation(existing) | map(attribute='name') | map('lower') | list %}
        {% if 'load_tstamp' not in names %}{% do legacy_tables.append(existing) %}{% endif %}
      {% endif %}
    {% endfor %}

    {% if legacy_tables %}
      {% set refresh_disabled = package_models
        | selectattr('config.materialized', 'equalto', 'incremental')
        | selectattr('config.full_refresh', 'sameas', false) | list %}
      {% if not flags.FULL_REFRESH or selected_models | length != package_models | length or refresh_disabled %}
        {{ exceptions.raise_compiler_error(
          'Upgrading snowplow_identities from 0.1.0 requires a package-scoped --full-refresh. '
          ~ 'Back up existing outputs and verify retained atomic history first, then run '
          ~ '`dbt run --full-refresh --select package:snowplow_identities`. '
          ~ 'Include every enabled package model and remove any full_refresh: false override. See MIGRATION.md.'
        ) }}
      {% endif %}
    {% endif %}
  {% endif %}
{% endmacro %}
