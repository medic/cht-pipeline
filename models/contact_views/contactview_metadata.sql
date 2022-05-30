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

SELECT * FROM(
SELECT
    "@timestamp"::timestamp without time zone AS "@timestamp",
    raw_contacts.doc ->> '_id'::text AS uuid,
    raw_contacts.doc ->> 'name'::text AS name,
    raw_contacts.doc ->> 'type'::text AS type,
    raw_contacts.doc ->> 'contact_type'::text    AS contact_type,
    raw_contacts.doc ->> 'phone'::text AS phone,
    raw_contacts.doc ->> 'alternative_phone'::text AS phone2,
    raw_contacts.doc ->> 'date_of_birth'::text AS date_of_birth,
    raw_contacts.doc #>> '{contact,_id}'::text[] AS contact_uuid,
    raw_contacts.doc #>> '{parent,_id}'::text[] AS parent_uuid,
    raw_contacts.doc ->> 'is_active'::text       AS active,
    raw_contacts.doc ->> 'notes'::text AS notes,
    '1970-01-01 03:00:00+03'::timestamp with time zone +
    (((raw_contacts.doc ->> 'reported_date'::text)::numeric) / 1000::numeric)::double precision *
    '00:00:01'::interval AS reported
FROM {{ ref("raw_contacts") }}
{% if is_incremental() %}
        WHERE COALESCE("@timestamp" > (SELECT MAX({{ this }}."@timestamp") FROM {{ this }}), True)
    {% endif %}
) as x
