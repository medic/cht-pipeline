{{
  config(
    materialized = 'incremental',
  )
}}

SELECT 
  ch.branch_uuid,
  ch.branch_name,
  ch.supervisor_uuid,
  ch.supervisor_name,
  ch.chw_area_uuid,
  ch.chw_uuid,
  ch.chw_name,
  ch.chw_phone,
  ch.area,
  ch.region,
  ch."@timestamp" AS "@timestamp"
FROM
  {{ ref("contactview_hierarchy") }} ch
INNER JOIN
  {{ ref("contact") }} cm 
ON
  ch.chw_uuid = cm.uuid
WHERE
  branch_name != 'HQ' AND branch_name != 'HQ OVC'
{% if is_incremental() %}
  AND ch."@timestamp" >= (select coalesce(max("@timestamp"), '1900-01-01') from {{ this }})
{% endif %}
