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

WITH source_records AS (
  SELECT
    _id as uuid,
    _deleted,
    saved_timestamp,
    doc->>'type' as doc_type,
  split_part(source, '/', 1) AS instance,
  split_part(source, '/', 2) AS dbname
  FROM {{ source('couchdb', env_var('POSTGRES_TABLE')) }} source_table
)

{{ batch_incremental('source_records') }}
