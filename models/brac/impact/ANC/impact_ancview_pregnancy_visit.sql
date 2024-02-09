{{ config(
  materialized='incremental',
  description='ANC Pregnancy Visit view for Brac Uganda'
) }}

SELECT
  uuid,
  source_id AS pregnancy_id,
  patient_id,
  form,
  reported_by,
  reported_by_parent,
  danger_signs AS visit_with_danger_sign,
  reported
FROM
  {{ ref('useview_visit') }} as visit
WHERE
  visit_type = 'anc'
  {% if is_incremental() %}
    and visit.reported > (select max(reported) from {{ this }})
  {% endif %}
