{{
    config(
        materialized = 'incremental',
        indexes=[
            {'columns': ['uuid']},
            {'columns': ['name']}
        ]
    )
}}

SELECT
    contactview_hospital.uuid,
    contactview_hospital.name,
    couchdb.doc->>'area' AS area,
    couchdb.doc->>'region' AS region,
    "@timestamp"::timestamp without time zone AS "@timestamp"
FROM
    {{ ref("contactview_hospital") }}
    INNER JOIN {{ ref("couchdb") }} ON (couchdb.doc ->> '_id'::text = contactview_hospital.uuid AND couchdb.doc ->> 'type' = 'district_hospital')

    {% if is_incremental() %}
        WHERE contactview_branch."@timestamp" > {{ max_existing_timestamp('"@timestamp"', target_ref=ref("contactview_chw")) }}
    {% endif %}