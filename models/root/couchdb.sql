{{
    config(
        materialized = 'incremental',
        unique_key='uuid',
        indexes=[
            {'columns': ['"@timestamp"'], 'type': 'brin'},
            {'columns': ['reported_date'], 'type': 'brin'},
            {'columns': ['type']},
            {'columns': ['contact_uuid']},
            {'columns': ['parent_uuid']},
            {'columns': ['uuid']},
        ]
    )
}}
WITH max_timestamp AS (
  DO $$
  BEGIN
    IF EXISTS (SELECT 1 FROM PG_CATALOG.PG_TABLES WHERE TABLENAME = 'couchdb') THEN
      EXECUTE 'SELECT coalesce(max("@timestamp"), ''1900-01-01'') AS max_timestamp FROM dbt.couchdb';
    ELSE
      EXECUTE 'SELECT * FROM (values(''1900-01-01'')) AS dummy(max_timestamp)';
    END IF;
  END $$;
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
{% if is_incremental() %}
  WHERE "@timestamp" >= SELECT max_timestamp from max_timestamp
{% endif %}
