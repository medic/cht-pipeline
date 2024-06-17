{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    indexes=[
      {'columns': ['"uuid"'], 'type': 'hash'},
      {'columns': ['"@timestamp"']},
      {'columns': ['reported']},
      {'columns': ['parent_uuid']},
      {'columns': ['contact_type']},
    ]
  )
}}

SELECT
  _id as uuid,
  "@timestamp",
  to_timestamp((NULLIF(doc ->> 'reported_date'::text, ''::text)::bigint / 1000)::double precision) AS reported,
  doc->'parent'->>'_id' AS parent_uuid,
  doc->>'name' AS name,
  COALESCE(doc->>'contact_type', doc->>'type') as contact_type,
  doc->>'phone' AS phone,
  doc->>'alternative_phone' AS phone2,
  doc->>'is_active' AS active,
  doc->>'notes' AS notes,
  doc->>'contact_id' AS contact_id
FROM {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }}
WHERE doc->>'type' IN ('contact', 'clinic', 'district_hospital', 'health_center', 'person')
{% if is_incremental() %}
  AND "@timestamp" >= {{ max_existing_timestamp('"@timestamp"') }}
{% endif %}
