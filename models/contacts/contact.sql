{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    indexes=[
      {'columns': ['"@timestamp"'], 'type': 'btree'},
      {'columns': ['reported'], 'type': 'brin'},
      {'columns': ['contact_uuid']},
      {'columns': ['parent_uuid']},
      {'columns': ['type']},
      {'columns': ['uuid']},
    ]
  )
}}

SELECT
  doc ->> '_id'::text AS uuid,
  doc ->> 'type'::text AS type,
  doc ->> 'name'::text AS name,
  doc ->> 'contact_type'::text AS contact_type,
  doc ->> 'phone'::text AS phone,
  doc ->> 'alternative_phone'::text AS phone2,
  doc ->> 'date_of_birth'::text AS date_of_birth,
  doc #>> '{contact,_id}'::text[] AS contact_uuid,
  doc #>> '{parent,_id}'::text[] AS parent_uuid,
  doc ->> 'is_active'::text AS active,
  doc ->> 'notes'::text AS notes,
  doc ->> 'reported_date'::text AS reported,
  doc ->> 'area'::text AS area,
  doc ->> 'region'::text AS region,
  doc ->> 'contact_id'::text AS contact_id,
  doc,
  "@timestamp"

FROM {{ ref('couchdb') }}
WHERE type = ANY
  (ARRAY ['contact'::text, 'clinic'::text, 'district_hospital'::text, 'health_center'::text, 'person'::text])
{% if is_incremental() %}
  AND "@timestamp" >= {{ max_existing_timestamp('"@timestamp"') }}
{% endif %}
