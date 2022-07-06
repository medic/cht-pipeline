{{
    config(
        materialized = 'incremental',
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
    doc->>'type' = 'data_record' AND
	doc#>>'{fields,patient_id}' IS NOT NULL
	{% if is_incremental() %}
        AND "@timestamp" > {{ max_existing_timestamp('"@timestamp"') }}
    {% endif %}
) x