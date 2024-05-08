{{
  config(
    materialized = 'incremental',
  )
}}

SELECT 
  contactview_chp.name as CHP_Name, contactview_chp.uuid as CHP_ID, contactview_branch.name as Branch_Name,
  contactview_chp.branch_uuid as Branch_ID,contactview_chp.phone as Phone, contactview_chp.phone2 as Phone2,
  contactview_chp.date_of_birth as DOB, contactview_chp.parent_type as Parent_Type, contactview_chp.area_uuid as Area_ID,        contactview_metadata.name AS supervisor_name
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
ORDER BY
  CHP_Name
