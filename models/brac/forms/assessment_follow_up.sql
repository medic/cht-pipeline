{{
    config(materialized = 'view')
}}

SELECT
  uuid,

  COALESCE(doc #>> '{fields,form_source_id}','') AS form_source_id,

  CASE
    WHEN (doc #>> '{fields,patient_age_in_years}') IS NULL OR (doc #>> '{fields,patient_age_in_years}') = ''
    THEN 99::int
    ELSE (doc #>> '{fields,patient_age_in_years}')::int
  END AS patient_age_in_years,

  CASE
    WHEN (doc #>> '{fields,patient_age_in_months}') IS NULL OR (doc #>> '{fields,patient_age_in_months}') = ''
    THEN 99::int
    ELSE (doc #>> '{fields,patient_age_in_months}')::int
  END AS patient_age_in_months,

  doc #>> '{fields,referral_follow_up_needed}' AS referral_follow_up_needed,
  doc #>> '{fields,follow_up_count}' AS follow_up_count,
  doc #>> '{fields,patient_health_facility_visit}' AS patient_health_facility_visit,
  doc #>> '{fields,group_followup_options,follow_up_type}' AS follow_up_type,
  doc #>> '{fields,group_followup_options,follow_up_method}' AS follow_up_method,
  doc #>> '{fields,danger_signs}' AS danger_signs,
  doc #>> '{fields,patient_improved}' AS patient_improved,
  doc #>> '{fields,patient_better}' AS patient_better,
  doc #>> '{fields,group_improved,g_patient_treatment_outcome}' AS g_patient_treatment_outcome,
  doc #>> '{fields,group_better,g_patient_better}' AS g_patient_referral_outcome

FROM
  {{ ref("data_record") }} AS form
WHERE
  form.form = 'assessment_follow_up'
