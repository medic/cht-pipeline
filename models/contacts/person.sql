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
    WHEN NULLIF(couchdb.doc->>'date_of_birth', '') IS NULL THEN NULL
    WHEN couchdb.doc->>'date_of_birth' ~ '^\d{4}-\d{2}-\d{2}$' THEN (couchdb.doc->>'date_of_birth')::date
    WHEN couchdb.doc->>'date_of_birth' ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(couchdb.doc->>'date_of_birth', 'DD/MM/YYYY')
    ELSE NULL
  END as date_of_birth,
  couchdb.doc->>'sex' as sex,
  couchdb.doc->>'phone' AS phone,
  couchdb.doc->>'alternative_phone' AS phone2,
  couchdb.doc->>'patient_id' as patient_id
FROM {{ ref("contact") }} contact
INNER JOIN {{ ref('contact_type') }} contact_type ON contact_type.id = contact.contact_type
INNER JOIN {{ source('couchdb', env_var('POSTGRES_TABLE')) }} couchdb ON couchdb._id = uuid
WHERE contact_type.person = true
{% if is_incremental() %}
  AND contact.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
