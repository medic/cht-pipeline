{{
    config(
        materialized = 'incremental',
		   indexes=[
            {'columns': ['"@timestamp"']}
        ]
    )
}}

SELECT
  "@timestamp"::timestamp without time zone AS "@timestamp",
	to_timestamp((doc#>>'{reported_date}')::double precision/1000)::timestamp AS reported,
	doc#>>'{fields,patient_id}' AS patient_id,
	doc->>'form' AS form,
	doc ->> '_id' AS uuid,
  doc#>>'{contact,_id}' AS reported_by,
	doc#>>'{contact,parent,_id}' AS reported_by_parent
FROM
	{{ ref("couchdb") }} as form
WHERE
    doc->>'type' = 'data_record' AND
	doc#>>'{fields,patient_id}' IS NOT NULL
  {% if is_incremental() %}
    and exists (
      select null
        from {{ this }} as this
        where this.uuid = form._id
        and form."@timestamp" > this."@timestamp"
    )
  {% endif %}
