{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['saved_timestamp'], 'type': 'btree'},
      {'columns': ['patient_id'], 'type': 'hash'},
    ]
  )
}}

SELECT
  uuid,
  person.saved_timestamp,
  couchdb.doc->>'patient_id' AS patient_id,
  person.deleted AS deleted
FROM {{ ref('person') }} person
INNER JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb ON couchdb._id = uuid
WHERE couchdb.doc->>'patient_id' IS NOT NULL
{% if is_incremental() %}
  AND person.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
