{{
  config(
    materialized = 'incremental',
  )
}}

SELECT
  branch.uuid AS branch_uuid,
  branch.name AS branch_name,
  chp.supervisor_uuid,
  cmeta.name AS supervisor_name,
  chp.area_uuid AS chw_area_uuid,
  chp.uuid AS chw_uuid,
  chp.name AS chw_name,
  chp.phone AS chw_phone,
  chp."@timestamp",
  branch.area,
  branch.region
FROM
  {{ ref("contactview_chp") }} AS chp
  JOIN {{ ref("contactview_branch") }}  AS branch ON chp.branch_uuid = branch.uuid
  JOIN {{ ref("contactview_metadata") }} AS cmeta ON cmeta.uuid = chp.supervisor_uuid

{% if is_incremental() %}
  WHERE "@timestamp" >= (SELECT MAX("@timestamp") FROM {{ this }})
{% endif %}

GROUP BY
  branch.uuid,
  branch.name,
  chp.supervisor_uuid,
  chp.area_uuid,
  cmeta.name,
  chp.uuid,
  chp.name,
  chp.phone,
  branch.area,
  branch.region