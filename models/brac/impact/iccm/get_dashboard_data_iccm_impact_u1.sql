{{ config(materialized = 'raw_sql') }}  

CREATE OR REPLACE FUNCTION {{ this }}
(
	param_facility_group_by text, 
	param_num_units text default '12', 
	param_interval_unit text default 'months', 
	param_include_current boolean default true
)
RETURNS TABLE(
		district_hospital_uuid text,
		district_hospital_name text,
		health_center_uuid text,
		health_center_name text,
		clinic_uuid text,
		clinic_name text,
		period_start date,
		period_start_epoch numeric,
		facility_join_field text,
		
		count_active_iccm numeric,
		
		count_u1_ax numeric,
		
		count_total_dx numeric,
		count_malaria_dx numeric,
		count_diarrhea_dx numeric,
		count_pneumonia_dx numeric,
		
		count_ax_with_fu_rec numeric,
		count_ax_with_fu_ref_rec numeric,
		count_ax_with_fu_tx_rec numeric,
		
		count_ax_with_fu_complete numeric,
		count_ax_with_fu_complete_on_time numeric,
		count_ax_with_fu_complete_patient_status_checked numeric,
		count_ax_with_fu_complete_patient_improved numeric,
		count_ax_with_fu_ref_complete numeric,
		count_ax_with_fu_ref_complete_attend_hf numeric,
		count_ax_with_fu_tx_complete numeric,
		count_ax_with_fu_tx_given_during_ax numeric,
		
		percent_ax_with_fu_complete double precision,
		percent_ax_with_fu_complete_on_time double precision,
		percent_ax_with_fu_complete_patient_improved double precision,
		percent_ax_with_fu_ref_complete double precision,
		percent_ax_with_fu_ref_complete_attend_hf double precision,		
		percent_ax_with_fu_tx_complete double precision,
		percent_ax_with_fu_tx_given_during_ax double precision,
		
		count_ax_within_24 numeric,
		count_ax_within_48 numeric,
		count_ax_within_72 numeric,
		count_ax_beyond_72 numeric,
		
		percent_ax_within_24 double precision,
		percent_ax_within_48 double precision,
		percent_ax_within_72 double precision,
		percent_ax_beyond_72 double precision,
		
		count_fu numeric,
		count_ax_with_immediate_tx_rec numeric
					
	)
	LANGUAGE sql
	STABLE
AS $BODY$


WITH period_CTE AS
(
  SELECT generate_series(
                date_trunc(param_interval_unit,now() - (param_num_units||' '||param_interval_unit)::interval),
                CASE
                  WHEN param_include_current
                  THEN now()
                  ELSE now() - ('1 ' || param_interval_unit)::interval 
                END,
                ('1 '||param_interval_unit)::interval
              )::date AS start			
),first_follow_up_CTE AS /* grabbing the first follow-up for every assessment to see if it's on-time */
(
SELECT
	DISTINCT ON (patient_id)
	patient_id,
	uuid,
	source_id,
	reported,
	patient_condition_improved,
	patient_condition_reported,
	facility_attended
FROM
	{{ ref("u1_iccmview_assessment_follow_up") }} 
ORDER BY
	patient_id,
	reported asc

),first_referral_follow_up_CTE AS /* grabbing the first referral follow-ups to see if referral follow-up rates */
(
SELECT
	DISTINCT ON (patient_id)
	uuid,
	patient_id,
	original_assessment_id,
	reported,
	patient_condition_improved,
	facility_attended,
	fu_ref
FROM
	{{ ref("u1_iccmview_assessment_follow_up") }} 
WHERE
	(source_type = 'referral'AND original_assessment_id <> '') /* for Siaya */
	OR (fu_ref) /* for other projects */
ORDER BY
	patient_id,
	reported asc

), iccm_targets_config_CTE AS
(

SELECT
	((value#>>'{targets,on_time_follow_up}'))::integer AS on_time_cutoff
FROM
	{{ ref("configuration") }} 
WHERE
	key = 'iccm'
	AND value ? 'targets'

)


--######################
--MAIN QUERY STARTS HERE
--######################

/*parameterized group by*/
/*this version is specifically for mobile app config where 'clinic' shouldn't be a group by category...lowest level is health_center' */
SELECT
	CASE
		WHEN param_facility_group_by = 'clinic' OR param_facility_group_by = 'health_center' OR param_facility_group_by = 'district_hospital' 			
		THEN place_period.district_hospital_uuid
		ELSE 'All'
	END AS _district_hospital_uuid,
	CASE
		WHEN param_facility_group_by = 'clinic' OR param_facility_group_by = 'health_center' OR param_facility_group_by = 'district_hospital' 	
		THEN place_period.district_hospital_name
		ELSE 'All'
	END AS _district_hospital_name,
	CASE
		WHEN param_facility_group_by = 'clinic' OR param_facility_group_by = 'health_center' 
		THEN place_period.health_center_uuid
		ELSE 'All'
	END AS _health_center_uuid,			
	CASE
		WHEN param_facility_group_by = 'clinic' OR param_facility_group_by = 'health_center' 
		THEN place_period.health_center_name
		ELSE 'All'
	END AS _health_center_name,
	
	CASE
		WHEN param_facility_group_by = 'clinic'
		THEN 'All'
		ELSE 'All'
	END AS _clinic_uuid,			
	CASE
		WHEN param_facility_group_by = 'clinic'
		THEN 'All'
		ELSE 'All'
	END AS _clinic_name,
	
	place_period.period_start AS _period_start,
	date_part('epoch',place_period.period_start)::numeric AS _period_start_epoch,
	
	CASE
		WHEN param_facility_group_by = 'health_center'
			THEN place_period.health_center_uuid
		WHEN param_facility_group_by = 'district_hospital'
			THEN place_period.district_hospital_uuid
		ELSE 'All'
	END AS _facility_join_field,
		
	/* USE-CASE ENGAGEMENT, BY FORM SUBMISSION DATE from either view, may need to adjust */
	SUM(COALESCE(reported_by_parent.count_active_iccm,0)) AS count_active_iccm,


	/* COUNTS FROM u1_iccmview_assessment */
	SUM(COALESCE(assess_count.count_u1_ax,0)) AS count_u1_ax,
	SUM(COALESCE(assess_count.count_total_dx,0)) AS count_total_dx,
	SUM(COALESCE(assess_count.count_malaria_dx,0)) AS count_malaria_dx,
	SUM(COALESCE(assess_count.count_diarrhea_dx,0)) AS count_diarrhea_dx,
	SUM(COALESCE(assess_count.count_pneumonia_dx,0)) AS count_pneumonia_dx,
	
	SUM(COALESCE(assess_count.count_ax_with_fu_rec,0)) AS count_ax_with_fu_rec,
	SUM(COALESCE(assess_count.count_ax_with_fu_ref_rec,0)) AS count_ax_with_fu_ref_rec,
	SUM(COALESCE(assess_count.count_ax_with_fu_tx_rec,0)) AS count_ax_with_fu_tx_rec,
	
	SUM(COALESCE(assess_count.count_ax_with_fu_complete,0)) AS count_ax_with_fu_complete,
	SUM(COALESCE(assess_count.count_ax_with_fu_complete_on_time,0)) AS count_ax_with_fu_complete_on_time,
	SUM(COALESCE(assess_count.count_ax_with_fu_complete_patient_status_checked,0)) AS count_ax_with_fu_complete_patient_status_checked,
	SUM(COALESCE(assess_count.count_ax_with_fu_complete_patient_improved,0)) AS count_ax_with_fu_complete_patient_improved,
	SUM(COALESCE(assess_count.count_ax_with_fu_ref_complete,0)) AS count_ax_with_fu_ref_complete,
	SUM(COALESCE(assess_count.count_ax_with_fu_ref_complete_attend_hf,0)) AS count_ax_with_fu_ref_complete_attend_hf,
	SUM(COALESCE(assess_count.count_ax_with_fu_tx_complete,0)) AS count_ax_with_fu_tx_complete,
	SUM(COALESCE(assess_count.count_ax_with_fu_tx_given_during_ax,0)) AS count_ax_with_fu_tx_given_during_ax,
	
	SAFE_DIVIDE(SUM(COALESCE(assess_count.count_ax_with_fu_complete,0)),SUM(COALESCE(assess_count.count_ax_with_fu_rec,0)),2) AS percent_ax_with_fu_complete,
	SAFE_DIVIDE(SUM(COALESCE(assess_count.count_ax_with_fu_complete_on_time,0)),SUM(COALESCE(assess_count.count_ax_with_fu_rec,0)),2) AS percent_ax_with_fu_complete_on_time,
	SAFE_DIVIDE(SUM(COALESCE(assess_count.count_ax_with_fu_complete_patient_improved,0)),SUM(COALESCE(assess_count.count_ax_with_fu_complete_patient_status_checked,0)),2) AS percent_ax_with_fu_complete_patient_improved,
	SAFE_DIVIDE(SUM(COALESCE(assess_count.count_ax_with_fu_ref_complete,0)),SUM(COALESCE(assess_count.count_ax_with_fu_ref_rec,0)),2) AS percent_ax_with_fu_ref_complete,
	SAFE_DIVIDE(SUM(COALESCE(assess_count.count_ax_with_fu_ref_complete_attend_hf,0)),SUM(COALESCE(assess_count.count_ax_with_fu_ref_complete,0)),2) AS percent_ax_with_fu_ref_complete_attend_hf,
	SAFE_DIVIDE(SUM(COALESCE(assess_count.count_ax_with_fu_tx_complete,0)),SUM(COALESCE(assess_count.count_ax_with_fu_tx_rec,0)),2) AS percent_ax_with_fu_tx_complete,
	SAFE_DIVIDE(SUM(COALESCE(assess_count.count_ax_with_fu_tx_given_during_ax,0)),SUM(COALESCE(assess_count.count_ax_with_immediate_tx_rec,0)),2) AS percent_ax_with_fu_tx_given_during_ax,	
	
	SUM(COALESCE(assess_count.count_ax_within_24,0)) AS count_ax_within_24,
	SUM(COALESCE(assess_count.count_ax_within_48,0)) AS count_ax_within_48,
	SUM(COALESCE(assess_count.count_ax_within_72,0)) AS count_ax_within_72,
	SUM(COALESCE(assess_count.count_ax_beyond_72,0)) AS count_ax_beyond_72,
	
	SAFE_DIVIDE(SUM(COALESCE(assess_count.count_ax_within_24,0)),SUM(COALESCE(assess_count.count_u1_ax,0)),2) AS percent_ax_within_24,
	SAFE_DIVIDE(SUM(COALESCE(assess_count.count_ax_within_48,0)),SUM(COALESCE(assess_count.count_u1_ax,0)),2) AS percent_ax_within_48,
	SAFE_DIVIDE(SUM(COALESCE(assess_count.count_ax_within_72,0)),SUM(COALESCE(assess_count.count_u1_ax,0)),2) AS percent_ax_within_72,
	SAFE_DIVIDE(SUM(COALESCE(assess_count.count_ax_beyond_72,0)),SUM(COALESCE(assess_count.count_u1_ax,0)),2) AS percent_ax_beyond_72,

	/* COUNT FROM u1_iccmview_assessment_follow_up */

	SUM(COALESCE(follow_up_count.count_fu,0)) AS count_fu,
	
	/* Add-ons */
	
	SUM(COALESCE(assess_count.count_ax_with_immediate_tx_rec,0)) AS count_ax_with_immediate_tx_rec
		
FROM /*combination of hierarchy level and time-periods we are grouping by (last 12 mo, by branch)*/
	(
		SELECT
			district_hospital.uuid AS district_hospital_uuid,
			district_hospital.name AS district_hospital_name,
			health_center.uuid AS health_center_uuid,
			health_center.name AS health_center_name,
			period_CTE.start AS period_start
			
		FROM
			period_CTE,	
			{{ ref("contactview_metadata") }}  AS health_center 
			INNER JOIN {{ ref("contactview_metadata") }} AS district_hospital ON (health_center.parent_uuid = district_hospital.uuid)
					
		WHERE
			district_hospital.type ='district_hospital' 
			AND district_hospital.parent_uuid IS NULL
				 	
	) AS place_period

LEFT JOIN /* CHWs Engaged with iCCM (submitted at least one assessment or assessment_follow_up form)*/
	
	(
		SELECT
			sq.reported_by_parent AS reported_by_parent,
			date_trunc(param_interval_unit,sq.reported) AS reported_month,
			COUNT(distinct(sq.reported_by)) AS count_active_iccm
		FROM
			(SELECT
				uuid,
				reported_by,
				reported_by_parent,
				reported
			FROM
				{{ ref("u1_iccmview_assessment") }} 
			
			UNION ALL
			
			SELECT
				uuid,
				reported_by,
				reported_by_parent,
				reported
			FROM
				{{ ref("u1_iccmview_assessment_follow_up") }} ) AS sq
				
		GROUP BY
			reported_by_parent,
			reported_month
		
	    ) AS reported_by_parent ON (place_period.period_start = reported_by_parent.reported_month AND place_period.health_center_uuid = reported_by_parent.reported_by_parent)


LEFT JOIN /* Counts From Assessment Forms (& Associated 1st Follow Up) */

		(SELECT
			assess.reported_by_parent AS reported_by_parent,
			date_trunc(param_interval_unit,assess.reported)::date AS period_reported,
			
			COUNT(distinct(assess.reported_by)) AS count_reported_by,
			
			COUNT(assess.uuid) AS count_u1_ax,
			SUM((assess.malaria_dx)::int+(assess.diarrhea_dx)::int+(assess.pneumonia_dx)::int) AS count_total_dx,
			SUM((assess.malaria_dx)::int) AS count_malaria_dx,
			SUM((assess.diarrhea_dx)::int) AS count_diarrhea_dx,
			SUM((assess.pneumonia_dx)::int) AS count_pneumonia_dx,
			
			SUM((assess.fu_rec)::int) AS count_ax_with_fu_rec,
			SUM((assess.fu_ref_rec)::int) AS count_ax_with_fu_ref_rec,
			SUM((assess.fu_tx_rec)::int) AS count_ax_with_fu_tx_rec,
			
			SUM((fu.uuid IS NOT NULL)::int) AS count_ax_with_fu_complete,
			SUM((fu.uuid IS NOT NULL AND fu.reported::date - assess.reported::date < config.on_time_cutoff)::int) AS count_ax_with_fu_complete_on_time,
			SUM((fu.uuid IS NOT NULL AND fu.patient_condition_reported)::int) AS count_ax_with_fu_complete_patient_status_checked,
			SUM((fu.uuid IS NOT NULL AND assess.fu_tx_rec AND fu.patient_condition_improved)::int) AS count_ax_with_fu_complete_patient_improved,

			SUM(((ref.uuid IS NOT NULL) AND assess.fu_ref_rec)::int) AS count_ax_with_fu_ref_complete,
			SUM(((ref.uuid IS NOT NULL) AND assess.fu_ref_rec AND ref.facility_attended)::int) AS count_ax_with_fu_ref_complete_attend_hf,
			SUM((fu.uuid IS NOT NULL AND assess.fu_tx_rec)::int) AS count_ax_with_fu_tx_complete,
			SUM((assess.fu_tx_needed_during_ax)::int) AS count_ax_with_immediate_tx_rec,
			SUM((assess.fu_tx_given_during_ax)::int) AS count_ax_with_fu_tx_given_during_ax,
				
			SUM((assess.within_24)::int) AS count_ax_within_24,
			SUM((assess.within_25_to_48 OR assess.within_24)::int) AS count_ax_within_48,
			SUM((assess.within_49_to_72 OR assess.within_25_to_48 OR assess.within_24)::int) AS count_ax_within_72,
			SUM((assess.beyond_72)::int) AS count_ax_beyond_72
			
		
		FROM
			iccm_targets_config_CTE AS config,
			{{ ref("u1_iccmview_assessment") }}  AS assess
			LEFT JOIN first_follow_up_CTE AS fu ON (fu.source_id = assess.uuid)
			LEFT JOIN first_referral_follow_up_CTE AS ref ON (assess.uuid = ref.original_assessment_id)
		
		GROUP BY
			reported_by_parent,
			period_reported
		
		) AS assess_count ON (place_period.period_start = assess_count.period_reported AND place_period.health_center_uuid = assess_count.reported_by_parent)


LEFT JOIN /* Counts from Assessment Follow Ups Forms */

		(SELECT
			reported_by_parent,
			date_trunc(param_interval_unit,reported)::date AS period_reported,
			
			--COUNT(distinct(reported_by)) AS count_reported_by,
			COUNT(uuid) AS count_fu,
			SUM((fu_ref)::int) AS count_fu_ref
		
		FROM
			{{ ref("u1_iccmview_assessment_follow_up") }} 
		GROUP BY
			reported_by_parent,
			period_reported
		
		) AS follow_up_count ON (place_period.period_start = follow_up_count.period_reported AND place_period.health_center_uuid = follow_up_count.reported_by_parent)
			

GROUP BY
	_district_hospital_uuid,
	_district_hospital_name,
	_health_center_uuid,
	_health_center_name,
	_clinic_uuid,
	_clinic_name,
	_period_start,
	_period_start_epoch,
	_facility_join_field
		
ORDER BY
	_district_hospital_name,
	_health_center_name,
	_clinic_name,
	_period_start


$BODY$