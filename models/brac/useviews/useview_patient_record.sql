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