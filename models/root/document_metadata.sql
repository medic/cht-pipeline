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
      {'columns': ['isntance']},
      {'columns': ['dbname']},
    ]
  )
}}

SELECT
  _id as uuid,
  _deleted,
  saved_timestamp,
  doc->>'type' as doc_type,
  split_part(source, '/', 1) AS instance,
  split_part(source, '/', 2) AS dbname
from {{ source('couchdb', env_var('POSTGRES_TABLE')) }} source_table
{% if is_incremental() %}
WHERE source_table.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
