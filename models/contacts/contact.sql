{{
  config(
    materialized = 'incremental',
    indexes=[
      {{{ var('columns') }}: ['"@timestamp"'], 'type': 'btree'},
      {{{ var('columns') }}: ['reported'], 'type': 'brin'},
      {{{ var('columns') }}: ['contact_uuid']},
      {{{ var('columns') }}: ['parent_uuid']},
      {{{ var('columns') }}: ['type']},
      {{{ var('columns') }}: ['uuid']},
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
  AND "@timestamp" >= (select coalesce(max("@timestamp"), '1900-01-01') from {{ this }})
{% endif %}
