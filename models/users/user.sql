{{
  config(
    materialized = 'incremental',
    unique_key='user_id',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['user_id'], 'type': 'hash'},
      {'columns': ['saved_timestamp']},
    ]
  )
}}

SELECT
  document_metadata.uuid as user_id,
  document_metadata.saved_timestamp,
  COALESCE(
    doc->>'contact_id',
    doc->>'facility_id'
  ) as contact_uuid,
  doc->>'language' as language,
  doc->>'roles' as roles
FROM {{ ref('document_metadata') }} document_metadata
INNER JOIN
  {{ source('couchdb', env_var('POSTGRES_TABLE')) }} source_table
  ON source_table._id = document_metadata.uuid
WHERE
  document_metadata.doc_type = 'user-settings'
  AND document_metadata._deleted = false
{% if is_incremental() %}
  AND document_metadata.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
