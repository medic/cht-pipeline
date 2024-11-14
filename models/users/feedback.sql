{% set COLUMNS = 'columns' %}
{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    indexes=[
      {COLUMNS: ['uuid'], 'type': 'hash'},
      {COLUMNS: ['saved_timestamp']},
      {COLUMNS: ['period_start']},
      {COLUMNS: ['user_name']},
    ]
  )
}}

SELECT
  document_metadata.uuid as uuid,
  document_metadata.saved_timestamp,
  doc#>>'{meta,source}' AS SOURCE,    
  doc#>>'{meta,url}' AS url,
  doc#>>'{meta,user,name}' AS user_name,
  doc#>>'{meta,time}' AS period_start,
  COALESCE(doc#>>'{info,cause}',doc->>'info') AS cause,
  doc#>>'{info,message}' AS message
FROM {{ ref('document_metadata') }} document_metadata
INNER JOIN
  {{ source('couchdb', env_var('POSTGRES_TABLE')) }} source_table
  ON source_table._id = document_metadata.uuid
WHERE
  document_metadata.doc_type = 'feedback'
  AND document_metadata._deleted = false
{% if is_incremental() %}
  AND document_metadata.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
