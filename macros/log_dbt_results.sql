{% macro log_dbt_results(results) %}
  -- depends_on: {{ ref('dbt_results') }}
  {%- if execute -%}
    {%- set parsed_results = parse_dbt_results(results) -%}
    {%- if parsed_results | length  > 0 -%}
      {% set delete_dbt_results_query -%}
        delete from {{ ref('dbt_results') }}
      {%- endset -%}
      {% set insert_dbt_results_query -%}
        insert into {{ ref('dbt_results') }}
          (
            result_id,
            invocation_id,
            unique_id,
            database_name,
            schema_name,
            name,
            resource_type,
            status,
            execution_time,
            rows_affected
          ) values
          {%- for parsed_result_dict in parsed_results -%}
            (
              '{{ parsed_result_dict.get('result_id') }}',
              '{{ parsed_result_dict.get('invocation_id') }}',
              '{{ parsed_result_dict.get('unique_id') }}',
              '{{ parsed_result_dict.get('database_name') }}',
              '{{ parsed_result_dict.get('schema_name') }}',
              '{{ parsed_result_dict.get('name') }}',
              '{{ parsed_result_dict.get('resource_type') }}',
              '{{ parsed_result_dict.get('status') }}',
              {{ parsed_result_dict.get('execution_time') }},
              {{ parsed_result_dict.get('rows_affected') }}
            ) {{- "," if not loop.last else "" -}}
          {%- endfor -%}
      {%- endset -%}
      {%- do run_query(delete_dbt_results_query) -%}
      {%- do run_query(insert_dbt_results_query) -%}
    {%- endif -%}
  {%- endif -%}
  -- This macro is called from an on-run-end hook and therefore must return a query txt to run. Returning an empty string will do the trick
  {{ return ('') }}
{% endmacro %}
