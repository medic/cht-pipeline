{{ config(materialized = 'raw_sql') }} 

CREATE OR REPLACE FUNCTION {{ this }} (param_facility_group_by text DEFAULT 'All'::text, param_num_units text DEFAULT '12'::text, param_interval_unit text DEFAULT 'Months'::text, param_include_current boolean DEFAULT true)
 RETURNS TABLE(district_hospital_uuid text, district_hospital_name text, health_center_uuid text, health_center_name text, clinic_uuid text, clinic_name text, period_start date, period_start_epoch numeric, facility_join_field text, hh_registered numeric, total_hh_registered numeric, hh_total_visit NUMERIC, hh_visit numeric, percent_hh_visit double precision)
 LANGUAGE sql
 STABLE
AS $function$
 -- Filter date
 WITH hh_data AS (
    -- For Window function
    WITH main_data AS ( 
            WITH period_CTE AS
                (
                    SELECT generate_series(			
                        (SELECT 
                            CASE WHEN min(reported)>(now()-(param_num_units||' '||param_interval_unit)::INTERVAL) 
                            THEN date_trunc(param_interval_unit,now() - (param_num_units||' '||param_interval_unit)::interval)
                        ELSE 
                            date_trunc(param_interval_unit,min(reported))::date END FROM {{ ref("contactview_metadata") }} WHERE TYPE='clinic'), 																
                        CASE
                            WHEN param_include_current 
                            THEN now() 
                            ELSE now() - ('1 ' || param_interval_unit)::interval
                        END, 									
                        ('1 '||param_interval_unit)::interval
                    )::date AS start
                )	
            SELECT
                CASE
                    WHEN param_facility_group_by = 'clinic'
                    OR param_facility_group_by = 'health_center'
                    OR param_facility_group_by = 'district_hospital' THEN place_period.district_hospital_uuid
                    ELSE 'All'
                END AS _district_hospital_uuid,
                CASE
                    WHEN param_facility_group_by = 'clinic'
                    OR param_facility_group_by = 'health_center'
                    OR param_facility_group_by = 'district_hospital' THEN place_period.district_hospital_name
                    ELSE 'All'
                END AS _district_hospital_name,
                CASE
                    WHEN param_facility_group_by = 'clinic'
                    OR param_facility_group_by = 'health_center' THEN place_period.health_center_uuid
                    ELSE 'All'
                END AS _health_center_uuid,
                CASE
                    WHEN param_facility_group_by = 'clinic'
                    OR param_facility_group_by = 'health_center' THEN place_period.health_center_name
                    ELSE 'All'
                END AS _health_center_name,
                'All'::TEXT AS _clinic_uuid,
                'All'::TEXT AS _clinic_name,
                place_period.period_start AS _period_start,
                DATE_PART('epoch', place_period.period_start)::NUMERIC AS _period_start_epoch,
                CASE
                    WHEN param_facility_group_by = 'health_center' THEN place_period.health_center_uuid
                    WHEN param_facility_group_by = 'district_hospital' THEN place_period.district_hospital_uuid
                    ELSE 'All'
                END AS _facility_join_field,
                COALESCE(SUM(hhcount.hh_registered),0) AS hh_registered,
                COALESCE(SUM(hh_visit),0) AS hh_visit,
                COALESCE(SUM(hh_total_visit),0) AS hh_total_visit		
            FROM
                /*combination of hierarchy level and time-periods we are grouping by (last 12 mo by default, by branch)*/
                (
                    SELECT
                        district_hospital.uuid AS district_hospital_uuid,
                        district_hospital.name AS district_hospital_name,
                        health_center.uuid AS health_center_uuid,
                        health_center.name AS health_center_name,
                        period_CTE.start AS period_start
                    FROM
                        period_CTE,
                        {{ ref("contactview_metadata") }} AS health_center
                    INNER JOIN {{ ref("contactview_metadata") }} AS district_hospital ON
                        (health_center.parent_uuid = district_hospital.uuid)
                    WHERE
                        district_hospital.type = 'district_hospital'
                        AND district_hospital.name <> 'HQ'
                        AND district_hospital.name <> 'HW OVC' 
                ) AS place_period
            -- HH visit
            LEFT JOIN 
            (
                SELECT
                    date_trunc(param_interval_unit,reported)::date AS reported_month,
                    reported_by_parent,
                    COUNT(household_id) AS hh_total_visit,
                    COUNT(DISTINCT household_id) AS hh_visit
                FROM
                    {{ ref("hhview_visits") }}
                GROUP BY
                    reported_by_parent,
                    reported_month
            ) AS hh_visit ON (place_period.period_start=hh_visit.reported_month 
                AND place_period.health_center_uuid=hh_visit.reported_by_parent)
            -- HH Registered 
            LEFT JOIN (
                    SELECT
                        DATE_TRUNC(param_interval_unit, reported) AS reported_month,
                        parent_uuid,
                        COUNT(DISTINCT uuid) AS hh_registered
                    FROM
                        {{ ref("contactview_metadata") }}
                    WHERE
                        TYPE='clinic'
                    GROUP BY
                        reported_month,
                        parent_uuid 
                ) AS hhcount 
                ON 	(place_period.period_start = hhcount.reported_month	AND place_period.health_center_uuid = hhcount.parent_uuid)
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
        ) -- main_data
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
            SUM(hh_registered) OVER (PARTITION BY _facility_join_field ORDER BY _period_start) AS total_hh_registered,
            hh_visit,
            hh_total_visit
        FROM
            main_data 
	) --hh_data

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
        hh_total_visit,
		hh_visit,
		{{ ref("safe_divide") }}(hh_visit,total_hh_registered,2) AS percent_hh_visit
	FROM
		hh_data
	WHERE
		-- Filter the required data only.
		_period_start >= NOW()- ((1 + param_num_units::INT) || ' ' || param_interval_unit)::INTERVAL;
$function$
;
