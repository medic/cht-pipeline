{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    indexes=[
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['savedTimestamp'], 'type': 'btree'},
      {'columns': ['patient_id'], 'type': 'hash'},
    ]
  )
}}

SELECT
  uuid,
  person.savedTimestamp,
  couchdb.doc->>'patient_id' as patient_id
FROM {{ ref('person') }} person
INNER JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb ON couchdb._id = uuid
WHERE couchdb.doc->>'patient_id' IS NOT NULL
{% if is_incremental() %}
  AND person.savedTimestamp >= {{ max_existing_timestamp('savedTimestamp') }}
{% endif %}
