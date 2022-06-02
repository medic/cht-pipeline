{{
    config(
        materialized = 'incremental',
        unique_key='idx_useview_patient_record_reported_patient_id',
		   indexes=[
            {'columns': ['"@timestamp"']}              
        ]
    )
}}

SELECT
{{ dbt_utils.surrogate_key(['reported', 'patient_id']) }} AS idx_useview_patient_record_reported_patient_id,
*
FROM(
SELECT 
    "@timestamp"::timestamp without time zone AS "@timestamp",	
	to_timestamp((doc#>>'{reported_date}')::double precision/1000)::timestamp AS reported,
	doc#>>'{fields,patient_id}' AS patient_id,
	doc->>'form' AS form,
	doc#>>'{contact,_id}' AS reported_by,
	doc#>>'{contact,parent,_id}' AS reported_by_parent
FROM 
	{{ ref("couchdb") }}
WHERE 
	doc#>'{fields}' ? 'patient_id'
	{% if is_incremental() %}
        AND COALESCE("@timestamp" > (SELECT MAX({{ this }}."@timestamp") FROM {{ this }}), True)
    {% endif %}
) x