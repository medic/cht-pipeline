-- pncview_visits.sql

-- Create View: PNC Visits
{{ config(
    materialized='view',
    description='View for tracking PNC visits'
) }}

WITH pnc_visits AS (
    SELECT
        pnc.uuid AS uuid,
        pnc.patient_id AS patient_id,
        pnc.form AS form,
        pnc.reported_by AS reported_by,
        pnc.reported_by_parent AS reported_by_parent,
        pnc.reported::DATE AS pnc_visit_date,
        deliv.facility_delivery AS facility_delivery,
        deliv.delivery_date AS delivery_date,
        pnc.follow_up_count AS pnc_visit_number,
        pnc.reported AS reported,
        pnc.baby_danger_signs <> '' AS visit_with_danger_sign,
        (deliv.delivery_date + '42 days'::INTERVAL)::DATE AS pnc_period_end,
        CASE
            WHEN deliv.facility_delivery THEN TRUE
            WHEN NOT deliv.facility_delivery AND deliv.first_visit_on_time THEN TRUE
            WHEN deliv.facility_delivery IS NULL THEN FALSE
            ELSE FALSE
        END AS first_visit_on_time,
        pnc.reported::DATE <= ((deliv.delivery_date + '42 days'::INTERVAL)::DATE) AS within_pnc_period
    FROM
        {{ ref('useview_postnatal_care') }} AS pnc
    LEFT JOIN {{ ref('pncview_actual_enrollments') }} AS deliv ON (deliv.patient_id = pnc.patient_id)
    WHERE
        pnc.follow_up_count <> 'NaN'
        AND pnc.pregnancy_outcome <> 'miscarriage'
        AND pnc.patient_id <> ''
        AND pnc.patient_id IS NOT NULL
        AND pnc.follow_up_method = 'in_person'
)

-- Model: pncview_visits
SELECT
    *
FROM pnc_visits;
