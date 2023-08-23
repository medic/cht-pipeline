{{ config(
    materialized='view',
    description='View for tracking expected PNC enrollments'
) }}

WITH pnc_expected_enrollments AS (
    SELECT
        DISTINCT ON(preg.patient_id)
        preg.uuid AS pregnancy_id,
        preg.patient_id,
        preg.patient_id AS patient_uuid,
        preg.reported_by AS reported_by,
        preg.reported_by_parent AS reported_by_parent,
        preg.lmp::DATE AS lmp,
        preg.reported AS reported,
        (preg.lmp::DATE + '294 days'::INTERVAL)::DATE AS expected_enrollment_date,
        (preg.lmp::DATE + '84 days'::INTERVAL)::DATE AS first_trimester_end
    FROM
        {{ ref('ancview_pregnancy') }} AS preg
    WHERE
        preg.patient_id IS NOT NULL
        AND preg.patient_id <> ''
        AND preg.patient_id NOT IN (
            SELECT patient_id
            FROM {{ ref('useview_postnatal_care') }}
            WHERE pregnancy_outcome = 'miscarriage'
        )
    ORDER BY
        preg.patient_id ASC,
        preg.reported DESC
)

-- Model: pncview_expected_enrollments
SELECT
    *
FROM pnc_expected_enrollments;
