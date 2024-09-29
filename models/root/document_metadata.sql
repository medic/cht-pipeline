{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    post_hook='delete from {{this}} where _deleted=true',
    indexes=[
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['saved_timestamp']},
      {'columns': ['doc_type']},
      {'columns': ['_deleted']},
    ]
  )
}}

WITH source_table_CTE AS (
  SELECT
    _id as uuid,
    _deleted,
    saved_timestamp,
    doc->>'type' as doc_type
  FROM {{ source('couchdb', env_var('POSTGRES_TABLE')) }}
)

{% if var("start_timestamp") is not none and var("batch_size") is not none %}
  SELECT *
  FROM source_table_CTE
  WHERE saved_timestamp >= '{{ var("start_timestamp") }}'
  ORDER BY saved_timestamp
  LIMIT {{ var('batch_size') }}
{% else %}

  SELECT
    _id as uuid,
    _deleted,
    saved_timestamp,
    doc->>'type' as doc_type
  FROM {{ source('couchdb', env_var('POSTGRES_TABLE')) }} source_table
  {% if is_incremental() %}
    WHERE source_table.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
  {% endif %}
{% endif %}
