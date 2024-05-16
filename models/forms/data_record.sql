{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    indexes=[
      {'columns': ['"uuid"'], 'type': 'hash'},
      {'columns': ['"@timestamp"'], 'type': 'btree'},
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
  (doc #>> '{fields,patient_id}')::text AS patient_id,
  (doc #>> '{contact,_id}')::text as contact_uuid,
  (doc #>> '{contact,parent,_id}')::text as contact_parent_uuid,
  to_timestamp((NULLIF(doc ->> 'reported_date'::text, ''::text)::bigint / 1000)::double precision) AS reported,
  doc ->> 'form' as form,
  doc,
  "@timestamp"
FROM {{ ref('couchdb') }}
WHERE
  type = 'data_record'
{% if is_incremental() %}
    AND "@timestamp" >= (select coalesce(max("@timestamp"), '1900-01-01') from {{ this }})
 {% endif %}
