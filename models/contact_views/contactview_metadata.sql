{{
    config(
        materialized = 'incremental',
        indexes=[
            {'columns': ['contact_uuid']},
            {'columns': ['parent_uuid']},
            {'columns': ['type']},
            {'columns': ['uuid']},
            {'columns': ['"@timestamp"']}
        ]
    )
}}

SELECT
    "@timestamp"::timestamp without time zone AS "@timestamp",
    doc ->> '_id'::text AS uuid,
    doc ->> 'name'::text AS name,
    doc ->> 'type'::text AS type,
    doc ->> 'contact_type'::text    AS contact_type,
    doc ->> 'phone'::text AS phone,
    doc ->> 'alternative_phone'::text AS phone2,
    doc ->> 'date_of_birth'::text AS date_of_birth,
    doc #>> '{contact,_id}'::text[] AS contact_uuid,
    doc #>> '{parent,_id}'::text[] AS parent_uuid,
    doc ->> 'is_active'::text       AS active,
    doc ->> 'notes'::text AS notes,
    to_timestamp((NULLIF(doc ->> 'reported_date', '')::bigint / 1000)::double precision) AS reported
FROM {{ ref("raw_contacts") }}
{% if is_incremental() %}
WHERE "@timestamp" > {{ max_existing_timestamp('"@timestamp"') }}
{% endif %}
