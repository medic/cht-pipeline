{% macro get_field(pattern) %}
{# pattern: fields/inputs/source return '{fields,inputs,source}' #}
{% set list =  pattern.split('/') %}

{% set list_str %}{{ list|join(",") }}{% endset %}}
{% set field %}{{"'{" ~ list_str ~ "}'"}}{% endset %}}
{{ return(field) }}

{% endmacro %}


{% macro pg_functions_from_json_final(schema_json_str) %}
{% set schema_json_dict = fromjson(schema_json_str) %}

{% set pg_query %}
SELECT
    {% for field in schema_json_dict['properties'].keys() %}        
        {% set f = get_field(schema_json_dict['properties'][field]['pattern']) %}
        data::json #>> {{ f }} AS {{field}}
    {%- if not loop.last %},{% endif -%}
    {% endfor %}
FROM {{ ref('my_json') }}
{% endset %}
{{ pg_query }}

{% endmacro %}