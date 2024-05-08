{{
  config(
    materialized = 'view',
    indexes=[
      {'columns': ['contact_uuid']},
      {'columns': ['parent_uuid']},
      {'columns': ['type']},
      {'columns': ['uuid']},
    ]
  )
}}

SELECT
  uuid,
  name,
  type,
  contact_type,
  phone,
  phone2,
  date_of_birth,
  contact_uuid,
  parent_uuid,
  active,
  notes,
  '1970-01-01 03:00:00+03'::timestamp with time zone +
  (((reported_date)::numeric) / 1000::numeric)::double precision *
  '00:00:01'::interval AS reported
FROM {{ ref("couchdb") }}
WHERE type = ANY
  (ARRAY ['contact'::text, 'clinic'::text, 'district_hospital'::text, 'health_center'::text, 'person'::text])
