-- a macro defining the reusable columns for all contact models
{% macro contact_columns() %}
  contact.uuid as uuid,
  contact.saved_timestamp,
  contact.parent_uuid,
  contact.reported,
  contact.name
{% endmacro %}

-- a macro defining the reusable indexes for columns above
{% macro contact_indexes() %}
  {{ return([
    {'columns': ['uuid'], 'type': 'hash'},
    {'columns': ['saved_timestamp']},
    {'columns': ['parent_uuid']},
    {'columns': ['reported']},
    {'columns': ['name']}
  ])}}
{% endmacro %}

-- this macro creates a contact model
-- contact_type: the id of the contact_type to be selected
-- parents: a list of parent contacts to join to this table, in this format
--    [{'id': '', 'table': ''}, {'id': '', 'table':''}]
--  id: id of the contact_type that is this contacts parent
--  table: the table to join to; if the parent contact type has a custom model
--    otherwise, 'contact' to use the main contact table
-- custom_contact_columns: any columns specific to this contact model
-- custom_indexes: any indexes for the contact specific columns
{% macro cht_contact_model(contact_type, parents, custom_contact_columns, custom_indexes=[]) %}
  -- combine any contact specific indexes with the general
  {%- set all_indexes = contact_indexes() + custom_indexes -%}

  --- if parents have been given, add the hierarchy here
  {% set columns = [] %}
  {% set joins = [] %}
  {% for i in range(0, parents|length) %}
    {% set parent = parents[i] %}
    {% set prev_parent = parents[i - 1]['id'] if i > 0 else 'contact' %}

    {% set parent_table = parent['table'] %}
    {% set join_clause = "LEFT JOIN " ~ ref(parent_table) ~ " AS " ~ parent['id'] ~ " ON " ~ parent['id'] ~ ".uuid = " ~ prev_parent ~ ".parent_uuid" %}
    {% do joins.append(join_clause) %}

    {% set column = parent['id'] ~ ".uuid AS " ~ parent['id'] %}
    {% do columns.append(column) %}
  {% endfor %}

  -- the actual select; a combination of
  -- contact specific fields
  -- the common data record fields
  -- and the common data record from, join and where clause
  -- config common to all contact models
  {{
    config(
      materialized='incremental',
      unique_key='uuid',
      on_schema_change='append_new_columns',
      indexes=all_indexes
    )
  }}

  SELECT
    {{ contact_columns() }},
    {{ columns | join(',\n  ') }},
    {{ custom_contact_columns }}
  FROM {{ ref('contact') }} contact
    INNER JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb ON couchdb._id = contact.uuid
    {{ joins | join('\n') }}
  WHERE
    contact.contact_type = '{{ contact_type }}'
  {% if is_incremental() %}
    AND contact.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
  {% endif %}
{% endmacro %}
