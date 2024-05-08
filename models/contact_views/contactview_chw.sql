{{
  config(
    materialized = 'view',
  )
}}

SELECT
  chw.name,
  pplfields.uuid,
  pplfields.phone,
  pplfields.phone2,
  pplfields.date_of_birth,
  pplfields.parent_type,
  chwarea.uuid AS area_uuid,
  chwarea.parent_uuid AS branch_uuid
FROM {{ ref("contactview_person_fields") }} AS pplfields
INNER JOIN {{ ref("contactview_metadata") }} AS chw ON chw.uuid = pplfields.uuid
INNER JOIN {{ ref("contactview_metadata") }} AS chwarea ON chw.parent_uuid = chwarea.uuid
WHERE pplfields.parent_type = 'health_center'
