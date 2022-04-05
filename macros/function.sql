{% materialization raw_sql, default %}

  {%- set target_relation = api.Relation.create(
        identifier=this.identifier, schema=schema, database=database,
        type='view') -%}

  {{ run_hooks(pre_hooks) }}

  {% call statement('main') -%}
    {{ sql }}
  {%- endcall %}

  {{ adapter.commit() }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
