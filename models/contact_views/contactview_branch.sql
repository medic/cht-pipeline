{{
  config(
    materialized = 'view',
  )
}}

SELECT
  ch.uuid,
  ch.name,
  cm.area,
  cm.region
FROM
  {{ ref("contactview_hospital") }} AS ch
INNER JOIN {{ ref("contactview_metadata") }} AS cm
ON (cm.uuid = ch.uuid AND cm.type = 'district_hospital')
