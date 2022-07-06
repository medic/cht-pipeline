{{
    config(
        materialized = 'incremental',
        unique_key='uuid',
        indexes=[
            {'columns': ['visit_type']},
            {'columns': ['"@timestamp"']}

        ]
    )
}}

SELECT
        "@timestamp"::timestamp without time zone AS "@timestamp",
		(doc ->> '_id') AS uuid,
		doc->>'form' AS form,
		COALESCE((doc #>> '{fields,inputs,source_id}'),'') AS source_id,
		CASE
			WHEN doc ->> 'form' = 'pregnancy_visit' THEN 'anc'
			WHEN doc ->> 'form' =  'postnatal_care' THEN 'pnc'
			WHEN doc ->> 'form' = 'assessment' THEN 'iccm'
			WHEN doc ->> 'form' = 'immunization_follow_up' THEN 'imm'
		END AS visit_type,		
		NOT ((doc #>> '{fields,danger_signs}') IS NULL OR (doc #>> '{fields,danger_signs}') = '') AS danger_signs,							
		COALESCE((doc #>> '{fields,patient_id}'),'') AS patient_id,												
		COALESCE((doc #>> '{contact,_id}'),'') AS reported_by,
		COALESCE((doc #>> '{contact,parent,_id}'),'') AS reported_by_parent,
		to_timestamp((NULLIF(doc ->> 'reported_date', '')::bigint / 1000)::double precision) AS reported
				
	FROM
		 {{ ref("couchdb") }}
			
	WHERE doc ->> 'form'::text = ANY (ARRAY ['pregnancy_visit'::text, 'postnatal_care'::text, 'immunization_follow_up'::text])
		OR (doc ->> 'form' = 'assessment' AND (doc #>> '{fields,patient_age_in_years}')::int <= 5)

