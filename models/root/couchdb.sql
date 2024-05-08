{{
    config(
        materialized = 'view',
        indexes=[
            {'columns': ['"@timestamp"'], 'type': 'brin'},
            {'columns': ['type']},
            {'columns': ['contact_uuid']},
            {'columns': ['parent_uuid']},
            {'columns': ['uuid']},
        ]
    )
}}

SELECT
    doc->>'type' AS type,
    doc ->> 'name'::text AS name,
    doc ->> 'contact_type'::text AS contact_type,
    doc ->> 'phone'::text AS phone,
    doc ->> 'alternative_phone'::text AS phone2,
    doc ->> 'date_of_birth'::text AS date_of_birth,
    doc #>> '{contact,_id}'::text[] AS contact_uuid,
    doc #>> '{parent,_id}'::text[] AS parent_uuid,
    doc ->> 'is_active'::text AS active,
    doc ->> 'notes'::text AS notes,
    doc ->> 'reported_date'::text AS reported
    *
FROM v1.{{ env_var('POSTGRES_TABLE') }}
