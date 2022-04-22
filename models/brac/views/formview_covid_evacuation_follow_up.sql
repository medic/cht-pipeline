SELECT 
        doc ->> '_id'::text AS uuid,
        doc #>> '{fields,inputs,source}'::text[] AS source,
        doc #>> '{fields,inputs,source_id}'::text[] AS source_id,
        doc #>> '{contact,_id}' AS reported_by,
        doc #>> '{contact,parent,_id}' AS reported_by_parent,
        doc #>> '{fields,inputs,contact,_id}'::text[] AS patient_id,
        doc #>> '{fields,inputs,contact,name}'::text[] AS patient_name,
        NULLIF(NULLIF(doc #>> '{fields,patient_age_in_years }', ''), 'NaN')::int AS patient_age_in_years,
        doc #>> '{fields, follow_up, patient_evacuated}'::text[] AS patient_evacuated,
        doc #>> '{fields,ppe_risk }'::text[] AS ppe_risk,
        to_timestamp((NULLIF(doc ->> 'reported_date', '')::bigint / 1000)::double precision) AS reported
    FROM {{ ref("couchdb") }}
    WHERE doc ->> 'form' = 'covid_evacuation_follow_up'