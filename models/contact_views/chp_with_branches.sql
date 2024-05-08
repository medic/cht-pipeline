{{
  config(
    materialized = 'incremental',
  )
}}

SELECT
  chp.name as CHP_Name,
  chp.uuid as CHP_ID,
  branch.name as Branch_Name,
  chp.branch_uuid as Branch_ID,
  chp.phone as Phone,
  chp.phone2 as Phone2,
  chp.date_of_birth as DOB,
  chp.parent_type as Parent_Type,
  chp.area_uuid as Area_ID,
  metadata.name as supervisor_name,
  metadata.reported as reported
FROM
  {{ ref("contactview_chp") }} AS chp,
  {{ ref("contactview_branch") }} AS branch,
  {{ ref("contactview_metadata") }} AS metadata,
  {{ ref("contactview_metadata") }} cm
WHERE
  chp.branch_uuid = branch.uuid AND
  chp.supervisor_uuid = metadata.uuid AND
  chp.uuid = cm.contact_uuid AND
  branch.name != 'HQ' AND branch.name != 'HQ OVC'
{% if is_incremental() %}
    AND metadata.reported > (SELECT MAX(reported) FROM {{ this }}), True)
{% endif %}
ORDER BY
  CHP_Name
