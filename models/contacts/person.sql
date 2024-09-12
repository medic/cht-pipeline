{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['saved_timestamp']},
    ]
  )
}}

SELECT
  contact.uuid,
  contact.saved_timestamp,
  CASE
    WHEN couchdb.doc->>'date_of_birth' IS NULL OR couchdb.doc->>'date_of_birth' = '' THEN NULL
    WHEN trim(couchdb.doc->>'date_of_birth') = '' THEN NULL
    WHEN (couchdb.doc->>'date_of_birth') ~ '^\d{4}-\d{2}-\d{2}$' THEN (couchdb.doc->>'date_of_birth')::date
    ELSE NULL
  END as date_of_birth,
  couchdb.doc->>'sex' as sex
FROM {{ ref("contact") }} contact
INNER JOIN {{ source('couchdb', env_var('POSTGRES_TABLE')) }} couchdb ON couchdb._id = uuid
WHERE contact.contact_type = 'person'
{% if is_incremental() %}
  AND contact.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
