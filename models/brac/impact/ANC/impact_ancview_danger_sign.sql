{{ config(
  materialized='incremental',
  description='ANC Danger Signs view for Brac Uganda'
) }}

WITH danger_sign_CTE AS (
  SELECT
    uuid,
    form,
    patient_id,
    uuid AS pregnancy_id,
    reported_by,
    reported_by_parent,
    reported
  FROM
    {{ ref('ancview_pregnancy') }}
  WHERE
    danger_sign_at_reg

  UNION ALL

  SELECT
    uuid,
    form,
    patient_id,
    source_id AS pregnancy_id,
    reported_by,
    reported_by_parent,
    reported
  FROM
    {{ ref('useview_visit') }}
  WHERE
    visit_type = 'anc'
    AND danger_signs
)

SELECT
  uuid,
  form,
  pregnancy_id,
  patient_id,
  reported_by,
  reported_by_parent,
  reported
FROM
  danger_sign_CTE

{% if is_incremental() %}

  AND reported > (select max(reported) from {{ this }})

{% endif %}
