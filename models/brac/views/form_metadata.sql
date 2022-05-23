{{
    config(
        materialized = 'incremental',
        unique_key="uuid",
        indexes=[
            {'columns': ['reported']},
            {'columns': ['reported_by']},
            {'columns': ['chw']},
            {'columns': ['reported_by_parent']},
            {'columns': ['patient_id']},
            {'columns': ['form']},
            {'columns': ['formname']}            
        ]
    )
}}
SELECT * FROM(
SELECT
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
    AND (couchdb.doc ->> '_rev') != (SELECT {{ this }}.rev_id FROM {{ this }} WHERE {{ this }}.uuid = (couchdb.doc ->> '_id')))
{% endif %}
) x
