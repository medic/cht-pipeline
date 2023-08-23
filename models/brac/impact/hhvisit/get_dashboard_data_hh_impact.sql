WITH hh_data AS (
WITH main_data AS (
    WITH period_CTE AS (
    SELECT generate_series(
        CASE
        WHEN param_include_current
            THEN date_trunc(param_interval_unit, now() - (param_num_units || ' ' || param_interval_unit)::INTERVAL)
        ELSE
            date_trunc(param_interval_unit, min(reported))::DATE
        END,
        CASE
        WHEN param_include_current
            THEN now()
        ELSE now() - ('1 ' || param_interval_unit)::INTERVAL
        END,
        ('1 ' || param_interval_unit)::INTERVAL
    )::DATE AS start
    )

    SELECT
    CASE
        WHEN param_facility_group_by IN ('clinic', 'health_center', 'district_hospital')
        THEN place_period.district_hospital_uuid
        ELSE 'All'
    END AS _district_hospital_uuid,
    CASE
        WHEN param_facility_group_by IN ('clinic', 'health_center', 'district_hospital')
        THEN place_period.district_hospital_name
        ELSE 'All'
    END AS _district_hospital_name,
    CASE
        WHEN param_facility_group_by IN ('clinic', 'health_center')
        THEN place_period.health_center_uuid
        ELSE 'All'
    END AS _health_center_uuid,
    CASE
        WHEN param_facility_group_by IN ('clinic', 'health_center')
        THEN place_period.health_center_name
        ELSE 'All'
    END AS _health_center_name,
    'All'::TEXT AS _clinic_uuid,
    'All'::TEXT AS _clinic_name,
    place_period.period_start AS _period_start,
    date_part('epoch', place_period.period_start)::NUMERIC AS _period_start_epoch,
    CASE
        WHEN param_facility_group_by = 'health_center'
        THEN place_period.health_center_uuid
        WHEN param_facility_group_by = 'district_hospital'
        THEN place_period.district_hospital_uuid
        ELSE 'All'
    END AS _facility_join_field,
    COALESCE(sum(hhcount.hh_registered), 0) AS hh_registered,
    COALESCE(sum(hh_visit), 0) AS hh_visit
    FROM
    (
        SELECT
        district_hospital.uuid AS district_hospital_uuid,
        district_hospital.name AS district_hospital_name,
        health_center.uuid AS health_center_uuid,
        health_center.name AS health_center_name,
        period_CTE.start AS period_start
        FROM
        period_CTE,
        contactview_metadata AS health_center
        INNER JOIN contactview_metadata AS district_hospital ON
        (health_center.parent_uuid = district_hospital.uuid)
        WHERE
        district_hospital.type = 'district_hospital'
        AND district_hospital.name NOT IN ('HQ', 'HW OVC')
    ) AS place_period
    LEFT JOIN
    (
        SELECT
        reported_by_parent,
        date_trunc(param_interval_unit, reported)::DATE AS reported_month,
        count(DISTINCT household_id) AS hh_visit
        FROM
        hhview_visits
        GROUP BY
        reported_by_parent,
        reported_month
    ) AS hh_visit ON (place_period.period_start = hh_visit.reported_month AND place_period.health_center_uuid = hh_visit.reported_by_parent)
    LEFT JOIN (
    SELECT
        parent_uuid,
        date_trunc(param_interval_unit, reported) AS reported_month,
        count(DISTINCT uuid) AS hh_registered
    FROM
        contactview_metadata
    WHERE
        TYPE = 'clinic'
    GROUP BY
        reported_month,
        parent_uuid
    ) AS hhcount ON (place_period.period_start = hhcount.reported_month AND place_period.health_center_uuid = hhcount.parent_uuid)
    GROUP BY
    _district_hospital_uuid,
    _district_hospital_name,
    _health_center_uuid,
    _health_center_name,
    _clinic_uuid,
    _clinic_name,
    _period_start,
    _facility_join_field
    ORDER BY
    _district_hospital_name,
    _health_center_name,
    _clinic_name,
    _period_start
)

SELECT
    _district_hospital_uuid,
    _district_hospital_name,
    _health_center_uuid,
    _health_center_name,
    _clinic_uuid,
    _clinic_name,
    _period_start,
    _period_start_epoch,
    _facility_join_field,
    hh_registered,
    -- Cumulative sum over all months
    hh_visit,
    sum(hh_registered) OVER (PARTITION BY _facility_join_field ORDER BY _period_start) AS total_hh_registered
FROM
    main_data
)

SELECT
_district_hospital_uuid,
_district_hospital_name,
_health_center_uuid,
_health_center_name,
_clinic_uuid,
_clinic_name,
_period_start,
_period_start_epoch,
_facility_join_field,
hh_registered,
total_hh_registered,
hh_visit,
safe_divide(hh_visit, total_hh_registered, 2) AS percent_hh_visit
FROM
hh_data
WHERE
-- Filter the required data only.
_period_start >= now() - ((1 + param_num_units::INT) || ' ' || param_interval_unit)::INTERVAL;
