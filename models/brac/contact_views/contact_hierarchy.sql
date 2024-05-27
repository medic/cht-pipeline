{{
  config(
    materialized = 'incremental',
  )
}}

SELECT 
  branch_uuid,
  branch_name,
  supervisor_uuid,
  supervisor_name,
  chw_area_uuid,
  chw_uuid,
  chw_name,
  chw_phone,
  area,
  region
FROM
  {{ ref("contactview_hierarchy") }} ch
INNER JOIN
  {{ ref("contact") }} cm 
ON
  ch.chw_uuid = cm.uuid
WHERE
  branch_name != 'HQ' AND branch_name != 'HQ OVC'
{% if is_incremental() %}
  AND reported_date >= (SELECT MAX(reported_date) FROM {{ this }} WHERE reported_date IS NOT NULL)
{% endif %}
