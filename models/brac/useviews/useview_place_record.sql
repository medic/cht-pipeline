SELECT
    to_timestamp((doc#>>'{reported_date}')::double precision/1000)::timestamp AS reported,
    doc#>>'{fields,place_id}' AS place_id,
    doc->>'form' AS form_name,
    doc#>>'{contact,_id}' AS reported_by,
    doc#>>'{contact,parent,_id}' AS reported_by_parent
FROM
    {{ ref("couchdb") }}
WHERE
    doc->>'type' = 'data_record' AND
    doc#>>'{fields,place_id}' IS NOT NULL