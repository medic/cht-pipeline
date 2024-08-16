{{
  config(
    materialized = 'materialized_view',
    indexes=[
      {'columns': ['chw_uuid']},
      {'columns': ['clinic_uuid']},
      {'columns': ['health_center_uuid']},
      {'columns': ['district_hospital_uuid']},
    ]
  )
}}

SELECT
  chw.uuid as chw_uuid,
  clinic.uuid as clinic_uuid,
  health_center.uuid as health_center_uuid,
  district_hospital.uuid as district_hospital_uuid
FROM
  {{ref('contact')}} chw
  INNER JOIN {{ref('contact')}} clinic ON chw.parent_uuid = clinic.uuid
  LEFT JOIN {{ref('contact')}} health_center ON clinic.parent_uuid = health_center.uuid
  LEFT JOIN {{ref('contact')}} district_hospital ON health_center.parent_uuid = district_hospital.uuid
WHERE chw.contact_type = 'person' AND clinic.contact_type = 'clinic';
