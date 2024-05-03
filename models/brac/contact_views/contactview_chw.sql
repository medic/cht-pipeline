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
  pplfields.reported,
  pplfields."@timestamp",
  chwarea.uuid AS area_uuid,
  chwarea.parent_uuid AS branch_uuid
FROM 
  {{ ref("contactview_person_fields") }} pplfields
  JOIN {{ ref("contactview_metadata") }} chw ON chw.uuid = pplfields.uuid
  JOIN {{ ref("contactview_metadata") }} chwarea ON chw.parent_uuid = chwarea.uuid
WHERE pplfields.parent_type = 'health_center'::text

{% if is_incremental() %}
  AND pplfields."@timestamp" >= (SELECT MAX("@timestamp") FROM {{ this }})
{% endif %}
