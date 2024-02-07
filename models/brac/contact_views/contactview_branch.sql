SELECT
    contactview_hospital.uuid,
    contactview_hospital.name,
    couchdb.doc->>'area' AS area,
    couchdb.doc->>'region' AS region
FROM
    {{ ref("contactview_hospital") }}
    INNER JOIN {{ ref("couchdb") }} ON (couchdb.doc ->> '_id'::text = contactview_hospital.uuid AND couchdb.doc ->> 'type' = 'district_hospital')
    