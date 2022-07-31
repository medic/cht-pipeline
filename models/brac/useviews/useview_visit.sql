{{
    config(
        materialized = 'incremental',
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
		WHEN doc ->> 'form' = 'immunization_follow_up' THEN 'imm'
	END AS visit_type,		
	NOT ((doc #>> '{fields,danger_signs}') IS NULL OR (doc #>> '{fields,danger_signs}') = '') AS danger_signs,							
	COALESCE((doc #>> '{fields,patient_id}'),'') AS patient_id,												
	COALESCE((doc #>> '{contact,_id}'),'') AS reported_by,
	COALESCE((doc #>> '{contact,parent,_id}'),'') AS reported_by_parent,
	to_timestamp((NULLIF(doc ->> 'reported_date', '')::bigint / 1000)::double precision) AS reported
			
	FROM
		 dbt.couchdb
			
	WHERE doc ->> 'form' IN ('pregnancy_visit', 'postnatal_care', 'immunization_follow_up')

	{% if is_incremental() %}
        AND "@timestamp" > {{ max_existing_timestamp('"@timestamp"') }}
	{% endif %}

UNION ALL 

SELECT
	"@timestamp"::timestamp without time zone AS "@timestamp",
	(doc ->> '_id') AS uuid,
	doc->>'form' AS form,
	COALESCE((doc #>> '{fields,inputs,source_id}'),'') AS source_id,
	CASE
		WHEN doc ->> 'form' = 'assessment' THEN 'iccm'
	END AS visit_type,		
	NOT ((doc #>> '{fields,danger_signs}') IS NULL OR (doc #>> '{fields,danger_signs}') = '') AS danger_signs,							
	COALESCE((doc #>> '{fields,patient_id}'),'') AS patient_id,												
	COALESCE((doc #>> '{contact,_id}'),'') AS reported_by,
	COALESCE((doc #>> '{contact,parent,_id}'),'') AS reported_by_parent,
	to_timestamp((NULLIF(doc ->> 'reported_date', '')::bigint / 1000)::double precision) AS reported
			
	FROM
		 {{ ref("couchdb") }}
			
	WHERE (doc ->> 'form' = 'assessment' AND (doc #>> '{fields,patient_age_in_years}') != '' AND (nullif(doc #>> '{fields,patient_age_in_years}', ''))::int <= 5)

{% if is_incremental() %}
        AND "@timestamp" > {{ max_existing_timestamp('"@timestamp"') }}
{% endif %}