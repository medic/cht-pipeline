{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['saved_timestamp']},
      {'columns': ['reported']},
      {'columns': ['parent_uuid']},
      {'columns': ['contact_type']},
    ]
  )
}}

SELECT
  document_metadata.uuid as uuid,
  document_metadata.saved_timestamp,
  to_timestamp((NULLIF(doc ->> 'reported_date'::text, ''::text)::bigint / 1000)::double precision) AS reported,
  doc->'parent'->>'_id' AS parent_uuid,
  doc->>'name' AS name,
  COALESCE(doc->>'contact_type', doc->>'type') as contact_type,
  doc->>'is_active' AS active,
  doc->>'notes' AS notes,
  NULLIF(doc->> 'muted', '') AS muted
FROM {{ ref('document_metadata') }} document_metadata
INNER JOIN
  {{ source('couchdb', env_var('POSTGRES_TABLE')) }} source_table
  ON source_table._id = document_metadata.uuid
WHERE
  document_metadata.doc_type IN ('contact', 'clinic', 'district_hospital', 'health_center', 'person')
  AND document_metadata._deleted = false
{% if is_incremental() %}
  AND document_metadata.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
