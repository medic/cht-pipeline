{{
  config(
    materialized = 'view'
  )
}}

with latest_timestamp as (
  SELECT coalesce(max("@timestamp"), '1900-01-01') AS max_timestamp FROM {{ ref('stable_couchdb') }}
)

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
  *
FROM v1.{{ env_var('POSTGRES_TABLE') }}
WHERE "@timestamp" >= latest_timestamp
