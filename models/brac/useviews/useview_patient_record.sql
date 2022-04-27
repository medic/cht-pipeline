{{
    config(
        materialized = 'view',
        unique_key='idx_useview_patient_record_reported_patient_id'
    )
}}

SELECT
{{ dbt_utils.surrogate_key('reported', 'patient_id') }} AS useview_assessment_reported_age_uuid,
*
FROM(
SELECT 
    	
	to_timestamp((doc#>>'{reported_date}')::double precision/1000)::timestamp AS reported,
	doc#>>'{fields,patient_id}' AS patient_id,
	doc->>'form' AS form,
	doc#>>'{contact,_id}' AS reported_by,
	doc#>>'{contact,parent,_id}' AS reported_by_parent
FROM 
	{{ ref("couchdb") }}
WHERE 
	doc#>'{fields}' ? 'patient_id'
) x