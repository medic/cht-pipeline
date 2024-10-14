{{
  config(
    materialized = 'incremental',
    incremental_strategy='microbatch',
    event_time='saved_timestamp',
    begin='2020-01-01',
    batch_size='day'
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
