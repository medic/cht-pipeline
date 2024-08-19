-- a macro defining the reusable columns for all form models
{% macro data_record_columns() %}
  data_record.uuid as uuid,
  data_record.saved_timestamp,
  data_record.contact_uuid as reported_by,
  data_record.parent_uuid as reported_by_parent,
  data_record.reported
{% endmacro %}

-- a macro defining the reusable indexes for columns above
{% macro data_record_indexes() %}
  {{ return([
    {'columns': ['uuid'], 'type': 'hash'}, 
    {'columns': ['saved_timestamp']},
    {'columns': ['reported_by']},
    {'columns': ['reported_by_parent']},
    {'columns': ['reported']}
  ])}}
{% endmacro %}

-- the from, join and where condition common to form models
-- selects the form from data record by name
-- joins back to the source table to get the document
-- and adds an incremental condition
{% macro data_record_join(form_name) %}
  FROM {{ ref('data_record') }} data_record
  INNER JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb ON couchdb._id = data_record.uuid
  WHERE
    data_record.form = '{{ form_name }}'
  {% if is_incremental() %}
    AND data_record.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
  {% endif %}
{% endmacro %}

-- this macro creates a simple form model
-- form_name: the name of the form to be selected
-- form_columns: form specific columns
-- form_indexes: any indexes for the form specific columns
{% macro cht_form_model(form_name, form_columns, form_indexes=[]) %}
  {{ cht_form_multi([{'form_name': form_name, 'form_columns': form_columns}], form_indexes) }}
{% endmacro %}

-- this macro creates a model from a list of cht_forms
-- forms: [{ 'form_name': the name of the form to be selected,
--    'form_columns': form specific columns }]
-- form_indexes: any indexes for the form specific columns
-- each of the forms will be UNIONED together
-- and should have the same custom columns
{% macro cht_form_multi(forms, form_indexes=[]) %}
  -- combine any form specific indexes with the general 
  {%- set all_indexes = data_record_indexes() + form_indexes -%}

  -- config common to all form models
  {{
    config(
      materialized='incremental',
      unique_key='uuid',
      on_schema_change='append_new_columns',
      indexes=all_indexes
    )
  }}

  -- the actual select; a combination of
  -- form specific fields
  -- the common data record fields
  -- and the common data record from, join and where clause
  {% for form in forms %}
    SELECT
      {{ data_record_columns() }},
      {{ form.form_columns }}
    {{ data_record_join(form.form_name) }}
    -- if there is more than one form, union them together
    {% if not loop.last %}
    UNION
    {% endif %}
  {% endfor %}
{% endmacro %}
