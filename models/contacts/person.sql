{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    indexes=[
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['savedTimestamp']},
    ]
  )
}}

SELECT
  contact.uuid,
  contact.savedTimestamp,
  couchdb.doc->>'date_of_birth' as date_of_birth,
  couchdb.doc->>'sex' as sex
FROM {{ ref("contact") }} contact
INNER JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb ON couchdb._id = uuid
WHERE contact.contact_type = 'person'
{% if is_incremental() %}
  AND contact.savedTimestamp >= {{ max_existing_timestamp('savedTimestamp') }}
{% endif %}
