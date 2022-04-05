{% materialization raw_sql, default %}

  {%- set target_relation = api.Relation.create(
        identifier=identifier, schema=schema, database=database,
        type='raw_sql') -%}

  {% call statement('main') -%}
    {{ sql }}
  {%- endcall %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
