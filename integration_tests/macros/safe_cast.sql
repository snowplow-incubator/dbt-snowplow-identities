{#
  dbt renders a yaml list in a unit-test fixture as a square-bracket literal, which
  Databricks does not parse. Render lists as array(...) and leave every other value to
  dbt-spark's own safe_cast.
#}
{% macro databricks__safe_cast(field, type) %}
  {%- if field is iterable and field is not string and field is not mapping -%}
    cast(array({% for v in field %}{{ dbt.string_literal(dbt.escape_single_quotes(v)) }}{% if not loop.last %}, {% endif %}{% endfor %}) as {{ type }})
  {%- else -%}
    {{ dbt.spark__safe_cast(field, type) }}
  {%- endif -%}
{% endmacro %}
