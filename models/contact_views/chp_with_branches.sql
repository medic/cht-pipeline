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
  {{ ref("contactview_chp") }},
  {{ ref("contactview_branch") }},
  {{ ref("contactview_metadata") }},
  {{ ref("contactview_metadata") }} cm
WHERE
  contactview_chp.branch_uuid = contactview_branch.uuid AND
  contactview_chp.supervisor_uuid = contactview_metadata.uuid AND
  contactview_chp.uuid = cm.contact_uuid AND
  contactview_branch.name != 'HQ' AND contactview_branch.name != 'HQ OVC'
ORDER BY
  CHP_Name