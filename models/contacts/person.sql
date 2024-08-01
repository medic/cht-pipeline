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
  (couchdb.doc->>'date_of_birth')::date AS date_of_birth,
  couchdb.doc->>'sex' AS sex,
  contact.deleted AS deleted
FROM {{ ref("contact") }} contact
INNER JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb ON couchdb._id = uuid
WHERE contact.contact_type = 'person'
{% if is_incremental() %}
  AND contact.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
