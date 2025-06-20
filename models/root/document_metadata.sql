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
      {'columns': ['instance']},
      {'columns': ['dbname']},
    ]
  )
}}

{% if is_incremental() %}
{% if var("batch_size", none) is not none %}
( 
SELECT
  _id AS uuid,
  _deleted,
  saved_timestamp,
  doc->>'type' AS doc_type,
  split_part(source, '/', 1) AS instance,
  split_part(source, '/', 2) AS dbname
FROM {{ source('couchdb', env_var('POSTGRES_TABLE')) }} source_table
WHERE saved_timestamp > {{ max_existing_timestamp('saved_timestamp') }}
ORDER BY saved_timestamp
LIMIT {{ var('batch_size') }}
) UNION (
SELECT
  _id AS uuid,
  _deleted,
  saved_timestamp,
  doc->>'type' AS doc_type,
  split_part(source, '/', 1) AS instance,
  split_part(source, '/', 2) AS dbname
  FROM {{ source('couchdb', env_var('POSTGRES_TABLE')) }} source_table
  WHERE saved_timestamp = {{ max_existing_timestamp('saved_timestamp') }}
)
{% else %}
SELECT
  _id AS uuid,
  _deleted,
  saved_timestamp,
  doc->>'type' AS doc_type,
  split_part(source, '/', 1) AS instance,
  split_part(source, '/', 2) AS dbname
  FROM {{ source('couchdb', env_var('POSTGRES_TABLE')) }} source_table
  WHERE saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
{% else %}
SELECT
  _id AS uuid,
  _deleted,
  saved_timestamp,
  doc->>'type' AS doc_type,
  split_part(source, '/', 1) AS instance,
  split_part(source, '/', 2) AS dbname
  FROM {{ source('couchdb', env_var('POSTGRES_TABLE')) }} source_table
{% endif %}
