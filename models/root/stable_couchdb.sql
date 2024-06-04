{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    indexes=[
      {'columns': ['"@timestamp"'], 'type': 'btree'},
      {'columns': ['reported_date'], 'type': 'brin'},
      {'columns': ['type']},
      {'columns': ['contact_uuid']},
      {'columns': ['parent_uuid']},
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
  doc ->> 'reported_date'::text AS reported_date,
  doc ->> 'area'::text AS area,
  doc ->> 'region'::text AS region,
  doc ->> 'contact_id'::text AS contact_id,
  (doc #>> '{fields,patient_id}')::text AS patient_id,
  (doc #>> '{contact,_id}')::text as contact_uuid,
  (doc #>> '{contact,parent,_id}')::text as contact_parent_uuid,
  to_timestamp((NULLIF(doc ->> 'reported_date'::text, ''::text)::bigint / 1000)::double precision) AS reported,
  doc ->> 'form' as form,
  *
FROM v1.{{ env_var('POSTGRES_TABLE') }}
{% if is_incremental() %}
  WHERE "@timestamp" >= {{ max_existing_timestamp('"@timestamp"') }}
{% endif %}
