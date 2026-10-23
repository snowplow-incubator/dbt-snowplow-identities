{% macro check_upgrade_fixture() %}
  {% if not target.schema.startswith('gh_sp_identities_dbt_') or not target.schema.endswith('_upgrade') %}
    {{ exceptions.raise_compiler_error('Upgrade fixture checks requires an isolated CI upgrade schema.') }}
  {% endif %}

  {% set mapping = ref('snowplow_identities_identifier_mapping') %}
  {% set old_mapping = adapter.get_relation(database=mapping.database, schema=mapping.schema, identifier=mapping.identifier) %}
  {% if old_mapping is none or not old_mapping.is_table %}
    {{ exceptions.raise_compiler_error('Expected the populated 0.1.0 identifier_mapping table before upgrading.') }}
  {% endif %}
  {% set count = run_query('select count(*) from ' ~ old_mapping) %}
  {% if count.rows[0][0] == 0 %}
    {{ exceptions.raise_compiler_error('The 0.1.0 identifier_mapping upgrade fixture is empty.') }}
  {% endif %}

  {% for model in ['snowplow_identities_snowplow_id_mapping', 'snowplow_identities_merge_events'] %}
    {% set relation = ref(model) %}
    {% set old = adapter.get_relation(database=relation.database, schema=relation.schema, identifier=relation.identifier) %}
    {% if old is none %}
      {{ exceptions.raise_compiler_error('Missing 0.1.0 upgrade fixture: ' ~ model) }}
    {% endif %}
  {% endfor %}
{% endmacro %}
