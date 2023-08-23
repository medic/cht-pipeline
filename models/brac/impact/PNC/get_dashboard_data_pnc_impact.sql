{{ config(
  materialized='table',
  description='Dashboard data for PNC impact in BRAC Uganda'
) }}

WITH period_CTE AS (
  SELECT generate_series(
      date_trunc(param_interval_unit, now() - (param_num_units || ' ' || param_interval_unit)::interval),
      CASE
        WHEN param_include_current THEN now()
        ELSE now() - ('1 ' || param_interval_unit)::interval
      END,
      ('1 ' || param_interval_unit)::interval
    )::date AS start
)

SELECT
  CASE
    WHEN param_facility_group_by = 'clinic' OR param_facility_group_by = 'health_center' OR param_facility_group_by = 'district_hospital'
    THEN place_period.district_hospital_uuid
    ELSE 'All'
  END AS district_hospital_uuid,
  CASE
    WHEN param_facility_group_by = 'clinic' OR param_facility_group_by = 'health_center' OR param_facility_group_by = 'district_hospital'
    THEN place_period.district_hospital_name
    ELSE 'All'
  END AS district_hospital_name,
  CASE
    WHEN param_facility_group_by = 'clinic' OR param_facility_group_by = 'health_center'
    THEN place_period.health_center_uuid
    ELSE 'All'
  END AS health_center_uuid,
  CASE
    WHEN param_facility_group_by = 'clinic' OR param_facility_group_by = 'health_center'
    THEN place_period.health_center_name
    ELSE 'All'
  END AS health_center_name,
  'All' AS clinic_uuid,
  'All' AS clinic_name,
  place_period.period_start AS period_start,
  date_part('epoch', place_period.period_start)::numeric AS period_start_epoch,
  CASE
    WHEN param_facility_group_by = 'health_center'
    THEN place_period.health_center_uuid
    WHEN param_facility_group_by = 'district_hospital'
    THEN place_period.district_hospital_uuid
    ELSE 'All'
  END AS facility_join_field,
  /* Other fields... (add the remaining fields here) */
FROM
  (
    -- Subqueries and joins... (add the subqueries and joins here)
  ) AS place_period
GROUP BY
  district_hospital_uuid,
  district_hospital_name,
  health_center_uuid,
  health_center_name,
  clinic_uuid,
  clinic_name,
  period_start,
  period_start_epoch,
  facility_join_field
ORDER BY
  district_hospital_name,
  health_center_name,
  clinic_name,
  period_start;
