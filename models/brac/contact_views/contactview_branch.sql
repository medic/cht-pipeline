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
    uuid,
    name,
    area,
    region,
    timestamp_column
FROM (
    SELECT
        contactview_hospital.uuid,
        contactview_hospital.name,
        couchdb.doc->>'area' AS area,
        couchdb.doc->>'region' AS region,
        "@timestamp"::timestamp without time zone AS timestamp_column,
        ROW_NUMBER() OVER (PARTITION BY contactview_hospital.uuid ORDER BY couchdb."@timestamp" DESC) AS row_num
    FROM
        {{ ref("contactview_hospital") }} contactview_hospital
        INNER JOIN {{ ref("couchdb") }} couchdb ON (couchdb.doc ->> '_id' = contactview_hospital.uuid AND couchdb.doc ->> 'type' = 'district_hospital')
) AS subquery
WHERE 
    row_num = 1
    {% if is_incremental() %}
        AND timestamp_column > {{ max_existing_timestamp('"@timestamp"', target_ref=ref("couchdb")) }}
    {% endif %}
