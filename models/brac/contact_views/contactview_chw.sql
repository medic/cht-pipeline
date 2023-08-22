SELECT chw.name,
    pplfields.uuid,
    pplfields.phone,
    pplfields.phone2,
    pplfields.date_of_birth,
    pplfields.parent_type,
    chwarea.uuid AS area_uuid,
    chwarea.parent_uuid AS branch_uuid,
    pplfields."@timestamp" AS "@timestamp"
FROM {{ ref("contactview_person_fields") }} pplfields
    JOIN {{ ref("contactview_metadata") }} chw ON chw.uuid = pplfields.uuid
    JOIN {{ ref("contactview_metadata") }} chwarea ON chw.parent_uuid = chwarea.uuid
WHERE pplfields.parent_type = 'health_center'::text