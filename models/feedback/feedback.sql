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
  _id as uuid,
  saved_timestamp,
  doc#>>'{meta,source}' AS source,    
  doc#>>'{meta,url}' AS url,
  doc#>>'{meta,user,name}' AS user_name,
  doc#>>'{meta,time}' AS period_start,
  COALESCE(doc#>>'{info,cause}',doc->>'info') AS cause,
  doc#>>'{info,message}' AS message
FROM
  {{ source('couchdb', env_var('POSTGRES_TABLE')) }} source_table
WHERE
  doc->>'type' = 'feedback'
  AND _deleted = false
{% if is_incremental() %}
  AND source_table.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
