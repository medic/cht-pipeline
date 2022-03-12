SELECT
		doc->>'_id' AS uuid,
		doc->>'name' AS name,
		COALESCE(doc->>'patient_id',doc->>'_id') AS patient_id,
		COALESCE(doc->>'date_of_birth','') AS date_of_birth,
		COALESCE(doc->>'sex','') AS sex,
		COALESCE(doc->>'phone','') AS phone,
		COALESCE(doc#>>'{parent,_id}','') AS parent_uuid,
		COALESCE(doc ->> 'has_disability', '') AS has_disability,		
		to_timestamp((NULLIF(doc ->> 'reported_date', '')::bigint / 1000)::double precision) AS reported
		
	FROM
		{{ ref("couchdb") }}
	WHERE
		doc->>'type' = 'person'