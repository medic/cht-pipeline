{{ config(
    materialized='view',
    description='View for tracking PNC danger signs'
) }}

WITH pnc_danger_signs AS (
    SELECT
        pnc.uuid AS uuid,
        pnc.patient_id AS patient_id,
        pnc.reported_by AS reported_by,
        pnc.reported_by_parent AS reported_by_parent,
        pnc.form AS form,
        pnc.reported AS reported,
        CASE
            WHEN pnc.delivery_date <> '' THEN pnc.delivery_date::DATE
            ELSE pnc.reported::DATE
        END AS date_of_event
    FROM
        {{ ref('useview_postnatal_care') }} AS pnc
    WHERE
        pnc.baby_danger_signs <> ''
        AND pnc.follow_up_count <> 'NaN'
        AND pnc.pregnancy_outcome <> 'miscarriage'
        AND pnc.patient_id <> ''
        AND pnc.patient_id IS NOT NULL
)

-- Model: pncview_danger_sign
SELECT
    *
FROM pnc_danger_signs
