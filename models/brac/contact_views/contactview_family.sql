SELECT
  contactview_clinic.uuid,
  contactview_clinic.name,
  contactview_clinic.chw_uuid,
  contactview_clinic.created,
  contactview_family_survey.mosquito_nets,
  contactview_family_survey.hygeinic_toilet,
  contactview_family_survey.family_planning_method,
  contactview_family_survey.source_of_drinking_water
FROM {{ ref("contactview_clinic") }}
LEFT JOIN {{ ref("contactview_family_survey") }} ON contactview_clinic.uuid = contactview_family_survey.uuid;
