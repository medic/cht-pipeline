{% macro max_existing(max_field, target_ref=this) -%}
{#
  Attribution: https://gist.github.com/davehowell/1d6564875f35e58d8da14c9adbcb92da
#}
{#
    Gets the max existing value for some field in the target, or some other ref or source.
    Use in incremental models, inside `if is_incremental()` or `if execute` blocks,
    otherwise the dbt model will not compile.
    Useful where you have a primary key or other watermark field and want to construct SQL with
    that value determined at compile time.
    Why?
        When using the max value multiple times in a query it will be better than inlining multiple
        subqueries to fetch the same value, and in many cases, hardcoding a value in a where clause
        generates a significantly better execution plan.

    Params: max_field - string, the name of the field
            target_ref - string, pass in a call to ref or source to ensure dag dependency
                                defaults to this (current context model)
    Return: A literal string of the max value - will not have any quoting
    Usage:  To return dates use `max_existing_date`
            To return timestamps use `max_existing_timestamp`

            For integers:

            SELECT pk_column [,col2 ...]
            FROM source_table_ref
            WHERE True
            {%- if is_incremental() %}
                AND some_int_column > {{ max_existing('some_int_column') }}
            {%- endif  %}

            For strings - n.b. probably not useful to use strings as watermarks

            SELECT pk_column [,col2 ...]
            FROM source_table_ref
            WHERE True
            {%- if is_incremental() %}
                AND some_string_column > '{{ max_existing("some_string_column") }}'
            {%- endif  %}
#}
    {% call statement('get_max_existing', fetch_result=True)-%}

        SELECT max({{ max_field }}) as max_existing
        FROM {{ '"' ~ this.schema ~ '"' ~ '.' ~ '"' ~ this.name~ '"' if target_ref in [this] else target_ref }}

    {%- endcall %}
    {% set max_existing_field = load_result('get_max_existing').table.columns['max_existing'].values()[0] %}
    {% if max_existing_field == none %}
      {{ return('1990-01-01 23:00.000') }}
    {% else %}
      {{ return(max_existing_field) }}
    {% endif %}
{%- endmacro %}


{% macro _max_existing_multirow(max_field, group_by_field) -%}
{#
    Internal usage. Gets the max existing values for multiple groups in a target table.

    Use in incremental model,
        inside `if is_incremental()` or `if execute` otherwise
        the dbt model will not compile.
    Params:
        max_field -  column or valid SQL expression to be maxed
        group_by_field - column or valid SQL expression to use in group by clause
    Return: agate.Table.rows https://agate.readthedocs.io/en/1.6.1/api/table.html

    Usage:
        See the public version of this macro.
#}
    {% call statement('get_max_existing_multirow', fetch_result=True)-%}

        SELECT
            {{ group_by_field }} as group_by_field,
            max({{ max_field }}) AS max_existing
        FROM {{ this.schema }}.{{ this.name }}
        GROUP BY {{ group_by_field }}

    {%- endcall %}
    {% set result = load_result('get_max_existing_multirow') %}

    {{ return(result.table.rows) }}
{%- endmacro %}

{% macro _two_column_matrix_to_list_dicts(agate_rows, col1_name, col2_name) %}
{#
    Converts an agate.Table.rows to a list of dicts
    Params: an agate.Table.rows and names of the two columns
    Return: list of dicts with keys col1_name and col2_name
 #}
    {%- set list_dicts = [] -%}
    {%- for row in agate_rows -%}
        {{- list_dicts.append( {col1_name: row[0], col2_name : row[1] } ) -}}
    {%- endfor -%}
    {% for dict_val in list_dicts %}
        {{ log(dict_val) }}
    {%- endfor -%}
    {{ return(list_dicts)}}
{% endmacro %}


{% macro max_existing_multirow(max_field, group_by_field) %}
{#
    Gets the max existing values for multiple groups in a target table.
    Use in incremental models, inside `if is_incremental()` or `if execute`
    otherwise the dbt model will not compile.

    Params:
        max_field -  column or valid SQL expression to be maxed
        group_by_field - column or valid SQL expression to use in group by clause
    Note: expressions should not have column aliases
    Return: list of dicts where dict attributes have the keys 'type' and 'max_val'

    Usage:
        Iterate over the rows e.g.

        {%- for row in max_existing_multirow('pk_column', 'type_column') %}
            {{ row.pk_column }}
        {%- endfor %}

        Useful in an incremental model that selects from a `UNION ALL` across
        many source models where the unique_key watermark field might not be unique
        across those models. A surrogate key, hash or concatenated field in the target is not
        adequate to check watermarks across multiple sources.
        If you maintain some lineage indicator then it is possible to fetch all the max values
        e.g.
            Note for this example the fields 'type' and 'pk' in the target model store which parent
            table the row comes from and the respective watermark of that table,
            in this case a primary key. The watermark could also be some timestamp like "updated_at"


        {%- set types = ['ada', 'grace', 'rosie'] %}

        {%- if is_incremental() %}
            {%- set max_values_list = mathspace.max_existing_multirow('pk', 'type') %}
        {%- endif -%}

        {%- for t in types %}
            SELECT pk, col2, col3, coln FROM {{ ref(t) }}
            WHERE True

          {%- if is_incremental() %}
            AND pk > {{ lookup_max_value(max_values, type) }}
          {%- endif -%}

          {%- if not loop.last %}
            UNION ALL
          {%- endif %}

        {%- endfor %}

#}
    {% set agate_rows = _max_existing_multirow(max_field, group_by_field) %}
    {% set list_dicts = _two_column_matrix_to_list_dicts(agate_rows, 'type', 'max_val')  %}
    {{ return(list_dicts) }}
{% endmacro %}

{% macro lookup_max_value(max_values_list, lookup_value, lookup_key='type', return_key='max_val') %}
{#
    Use with the result of the max_existing_multirow macro.
    Does a lookup into that list of dicts for a given lookup_key.

    Params:  max_values - the output of the max_existing_multirow macro, a list of dicts
      must have a 'type' and 'max_val' key
    Return: literal string of the max value if found or None
    None will print blank and probably cause an error in the SQL of your model.
#}
    {{ return (max_values_list | selectattr(lookup_key, 'eq', lookup_value) | map(attribute=return_key) | list | first) }}
{% endmacro %}

{% macro _cast_as_type(stringvalue, as_type) %}
    {{ return("CAST('" ~ stringvalue ~ "' AS " ~ as_type ~ ")") }}
{% endmacro %}


{% macro max_existing_timestamp(timestampfield, target_ref=this) -%}
{#
    Params:
        timestampfield: date or timestamp
    Returns a SQL expression casting the max value string to a timestamp.
        e.g. "CAST('2019-01-01 23:00.000' AS TIMESTAMP)"
#}
    {{ return(_cast_as_type(max_existing(timestampfield, target_ref), 'TIMESTAMP')) }}
{%- endmacro %}


{% macro max_existing_date(timestampfield, target_ref=this) -%}
{#
    Params:
        timestampfield: date or timestamp
        target_ref: string, optional, defaults to the current context model
                    pass in a call to ref or source to target a different model
    Returns a SQL expression casting the max value string to a timestamp.
        e.g. "CAST('2019-01-01' AS DATE)"
    Usage:
        current model - latest value of a field called "updated_at":
            {{ max_existing_date('updated_at') }}

        some other model ref - latest value of a field called "deactivated_at":
            {{ max_existing_date('deactivated_at', ref('other_model')) }}

        some other model source - latest value of a field called "deactivated_at":
            {{ max_existing_date('deactivated_at', source('source_name', 'table_or_view_name')) }}

#}
    {{ return(_cast_as_type(max_existing(timestampfield, target_ref), 'DATE')) }}
{%- endmacro %}


{% macro _max_existing_prior_date(datefield, target_ref=this) -%}
    {% call statement('max_existing_prior_date', fetch_result=True)-%}

        SELECT {{ dbt_utils.dateadd('day', -1,  "max(" ~ datefield ~ ")" ) }} as max_existing_prior_date
        FROM {{ '"' ~ this.schema ~ '"' ~ '.' ~ '"' ~ this.name~ '"' if target_ref in [this] else target_ref }}

    {%- endcall %}
    {% set max_existing_prior = load_result('max_existing_prior_date').table.columns['max_existing_prior_date'].values()[0] %}
    {{ return(max_existing_prior) }}
{%- endmacro %}


{% macro max_existing_prior_date(datefield, target_ref=this) -%}
{#
    A day prior to the max of some date field.
    Params, Return and Usage as per the max_existing_date() macro
#}
    {{ return(_cast_as_type(_max_existing_prior_date(datefield, target_ref), 'DATE')) }}
{%- endmacro %}


{% macro max_existing_prior_timestamp(datefield, target_ref=this) -%}
{#
    A day prior to the max of some timestamp field
    Params, Return and Usage as per the max_existing_date() macro
#}
    {{ return(_cast_as_type(_max_existing_prior_date(datefield, target_ref), 'TIMESTAMP')) }}
{%- endmacro %}