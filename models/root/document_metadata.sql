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
    doc->>'type' as doc_type
  split_part(source, '/', 1) AS instance,
  split_part(source, '/', 2) AS dbname
  FROM {{ source('couchdb', env_var('POSTGRES_TABLE')) }} source_table
)

-- for incremental runs
-- select ALL records from source where document_metadata.saved_timestamp
-- is exactly equal to source.saved_timestamp
-- then union this with at most batch_size records
-- where document_metadata.source_timestamp is strictly greater than
-- document_metadata.saved_timestamp
-- this ensures that all records with the same timestamp are inserted
-- but limits later records to batch_size
{% if is_incremental() %}
  --define a CTE current_batch with a limit of batch_size
  , current_batch AS (
    SELECT * FROM source_records
    WHERE saved_timestamp > {{ max_existing_timestamp('saved_timestamp') }}
    {% if var("batch_size", none) is not none %}
      ORDER BY saved_timestamp
      LIMIT {{ var('batch_size') }}
    {% endif %}
  )

  -- union the CTE with a query getting all records with equal timestamp
  SELECT * FROM source_records
  WHERE saved_timestamp = {{ max_existing_timestamp('saved_timestamp') }}
  UNION ALL
  SELECT * FROM current_batch

-- if not incremental (the table is being created for the first time)
-- and batch size is defined
-- apply a limit of batch size so the entire table is not created
-- in a sinlge batch
{% else %}
  SELECT * FROM source_records
  {% if var("batch_size", none) is not none %}
    ORDER BY saved_timestamp
    LIMIT {{ var('batch_size') }}
  {% endif %}
{% endif %}
