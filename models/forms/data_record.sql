{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    indexes=[
      {'columns': ['"uuid"'], 'type': 'hash'},
      {'columns': ['"@timestamp"'], 'type': 'brin'},
      {'columns': ['"reported"'], 'type': 'brin'},
      {'columns': ['"patient_id"'], 'type': 'hash'},
      {'columns': ['"contact_uuid"'], 'type': 'hash'},
      {'columns': ['"contact_parent_uuid"'], 'type': 'hash'},
      {'columns': ['"form"'], 'type': 'hash'},
    ]
  )
}}

SELECT
  uuid,
  patient_id,
  contact_uuid,
  contact_parent_uuid,
  reported,
  form,
  doc,
  "@timestamp"
FROM {{ ref('stable_couchdb') }}
WHERE
  type = 'data_record'
{% if is_incremental() %}
  AND "@timestamp" >= {{ max_existing_timestamp('"@timestamp"') }}
{% endif %}
