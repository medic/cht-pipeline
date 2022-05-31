{{
    config(
        materialized = 'incremental',
        unique_key="uuid",
        indexes=[
            {'columns': ['reported']},
            {'columns': ['reported_by']},
            {'columns': ['rev_id']},
            {'columns': ['chw']},
            {'columns': ['reported_by_parent']},
            {'columns': ['patient_id']},
            {'columns': ['form']},
            {'columns': ['formname']},
            {'columns': ['"@timestamp"']}              
        ]
    )
}}

SELECT
        "@timestamp"::timestamp without time zone AS "@timestamp",
        doc ->> '_id' AS uuid,
        doc ->> '_rev' AS rev_id,
        doc #>> '{contact,_id}' AS reported_by,
        doc #>> '{contact,_id}' AS chw,
        doc #>> '{contact,parent,_id}' AS reported_by_parent,
        COALESCE(doc->>'patient_id',doc #>> '{fields,patient_id}') AS patient_id,
        doc ->> 'form' AS form,
        doc ->> 'form' AS formname,
        COALESCE((doc ->> 'errors'),'[]') != '[]' AS errors,
        to_timestamp((NULLIF(doc ->> 'reported_date', '')::bigint / 1000)::double precision) AS reported
        
    FROM
        {{ ref("couchdb") }} 
        
    WHERE
        (doc ->> 'type') = 'data_record'
        AND (doc #>> '{contact,_id}') IS NOT NULL
        AND (doc ->> 'form') IS NOT NULL

    {% if is_incremental() %}
        AND COALESCE("@timestamp" > (SELECT MAX({{ this }}."@timestamp") FROM {{ this }}), True)
    {% endif %}