{{
    config(
        materialized = 'incremental',
        indexes=[
            {'columns': ['patient_id']},
            {'columns': ['parent_uuid']},
            {'columns': ['"@timestamp"']}
        ]
    )
}}

SELECT
        "@timestamp"::timestamp without time zone AS "@timestamp",
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

        {% if is_incremental() %}
			AND "@timestamp" > COALESCE({{ max_existing_timestamp('"@timestamp"', target_ref=ref("couchdb")) }}, '1970-01-01 00:00:00'::timestamp)
		{% endif %}
