{{ config(
  materialized='table',
  description='ANC Pregnancy view for Brac Uganda'
) }}

WITH config_cte AS (
  SELECT ( value #>> '{lmp_calcs,maximum_days_pregnant}'::TEXT[])::INTEGER AS maximum_days_pregnant
  FROM configuration
  WHERE key = 'anc'::TEXT AND value ? 'lmp_calcs'::TEXT
)

SELECT
  preg.uuid AS uuid,
  preg.lmp::DATE,
  preg.edd,
  preg.imported,
  preg.patient_id,
  preg.chw AS reported_by,
  contact.parent_uuid AS reported_by_parent,
  preg.reported,
  'pregnancy'::TEXT AS form,
  preg.lmp <> '' AS has_lmp,
  date_trunc('day', preg.reported::TIMESTAMP WITH TIME ZONE) <= (preg.lmp::DATE + '84 days'::INTERVAL) AS early_reg,
  (preg.lmp::DATE + '84 days'::INTERVAL)::DATE AS first_tri_end,
  (preg.lmp::DATE + '168 days'::INTERVAL)::DATE AS second_tri_end,
  (preg.lmp::DATE + ((config.maximum_days_pregnant || ' days')::INTERVAL))::DATE AS mdd,
  date_part('days', now() - preg.lmp::DATE::TIMESTAMP WITH TIME ZONE)::INTEGER AS days_since_lmp,
  date_part('days', preg.reported - preg.lmp::DATE::TIMESTAMP WITHOUT TIME ZONE)::INTEGER AS days_pregnant_at_reg,
  preg.danger_signs <> ''::TEXT AS danger_sign_at_reg,
  preg.risk_factors <> ''::TEXT AS has_risk_factor
FROM config_cte AS config,
  {{ ref('useview_pregnancy') }} AS preg
INNER JOIN {{ ref('contactview_metadata') }} AS contact ON contact.uuid = preg.chw;
