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

SELECT
  _id as uuid,
  _deleted,
  saved_timestamp,
  doc->>'type' as doc_type
from {{ source('couchdb', env_var('POSTGRES_TABLE')) }} source_table
{% if var('start_timestamp') is not none and var('end_timestamp' is not none)%}
  WHERE source_table.saved_timestamp >= {{ var('start_timestamp') }} AND source_table.saved_timestamp <= {{ var('end_timestamp') }}
{% elif is_incremental() %}
  WHERE source_table.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
