{% materialization raw_sql, default %}

  {%- set target_relation = api.Relation.create(
        identifier=this, schema=schema, database=database,
        type='view') -%}

  {{ run_hooks(pre_hooks) }}

  {% call statement('main') -%}
    {{ sql }}
  {%- endcall %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
