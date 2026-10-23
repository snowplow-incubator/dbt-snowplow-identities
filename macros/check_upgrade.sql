{# Fail before changing any outputs when a 0.1.0 installation needs rebuilding. #}
{% macro check_upgrade() %}
  {% if execute and flags.WHICH in ['run', 'build'] and not flags.FULL_REFRESH %}
    {% set expected = ref('snowplow_identities_merge_events') %}
    {% set existing = adapter.get_relation(database=expected.database, schema=expected.schema, identifier=expected.identifier) %}
    {% if existing is not none %}
      {% set names = adapter.get_columns_in_relation(existing) | map(attribute='name') | map('lower') | list %}
      {% if 'load_tstamp' not in names %}
        {{ exceptions.raise_compiler_error('Upgrading snowplow_identities from 0.1.0 requires a package-scoped --full-refresh. Back up existing outputs and verify retained atomic history first. See MIGRATION.md.') }}
      {% endif %}
    {% endif %}
  {% endif %}
{% endmacro %}
