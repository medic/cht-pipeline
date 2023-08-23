{{ config(
  materialized='view',
  description='ANC Delivery view for Brac Uganda'
) }}

WITH preg AS (
  SELECT
    DISTINCT ON (patient_id)
    patient_id,
    reported,
    uuid
  FROM
    {{ ref('ancview_pregnancy') }}
  ORDER BY
    patient_id ASC,
    reported DESC
)

SELECT
  pnc.uuid AS uuid,
  preg.uuid AS pregnancy_id,
  pnc.patient_id AS patient_id,
  pnc.form AS form,
  pnc.chw AS reported_by,
  pnc.reported_by_parent AS reported_by_parent,
  pnc.delivery_date::DATE AS delivery_date,
  pnc.pregnancy_outcome,
  TRUE AS delivered,
  pnc.reported AS reported,
  (pnc.health_facility_delivery = 'yes') AS at_health_facility,
  (pnc.health_facility_delivery = 'yes') AS skilled_birth_attendant,
  (pnc.baby_danger_signs <> '' AND pnc.baby_danger_signs IS NOT NULL) AS baby_danger_signs
FROM
  {{ ref('useview_postnatal_care') }} AS pnc
LEFT JOIN preg ON (pnc.reported > preg.reported AND pnc.reported < (preg.reported + '1 year'::INTERVAL) AND pnc.patient_id = preg.patient_id)
WHERE
  pnc.follow_up_count = '1'
  AND (pnc.pregnancy_outcome IN (VALUES ('healthy'), ('still_birth')))
  AND pnc.patient_id IS NOT NULL AND pnc.patient_id <> '';
