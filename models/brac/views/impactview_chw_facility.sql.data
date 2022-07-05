SELECT chw.uuid AS chw_uuid,
    chw.name AS chw_name,
    chw.phone AS chw_phone,
    facility.uuid AS facility_join_field,
    facility.name AS facility_name
FROM ({{ ref("contactview_chp") }} chw
    JOIN {{ ref("contactview_metadata") }} facility ON ((chw.branch_uuid = facility.uuid)))
WHERE ((facility.name <> 'HQ'::text) AND (facility.name <> 'HQ OVC'::text))