SELECT
  contactview_clinic.uuid,
  contactview_clinic.name,
  contactview_clinic.chw_uuid,
  contactview_clinic.created,
  contactview_family_survey.solar_light,
  contactview_family_survey.water_filter,
  contactview_family_survey.children_under_5,
  contactview_family_survey.improved_cook_stove
FROM {{ ref("contactview_clinic") }}
LEFT JOIN {{ ref("contactview_family_survey") }} ON contactview_clinic.uuid = contactview_family_survey.uuid
