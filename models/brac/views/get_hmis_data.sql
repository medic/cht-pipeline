SELECT
	period_chp.uuid AS chp_uuid,
	period_chp.name,
	period_chp.date,
	period_chp.branch_uuid,
	period_chp.supervisor_uuid,
	period_chp.branch_name,
	period_chp.region,
	CASE WHEN COALESCE(num_of_hh_visited.count, 0) > 0 THEN 'Yes' ELSE 'No' END AS chp_active,
	COALESCE(cumulative_demography.total_households, 0) AS total_households,
	COALESCE(num_of_hh_covered.count, 0) AS households_registered,
	COALESCE(num_of_hh_visited.count, 0) AS num_of_hh_visited,
	COALESCE(family_survey.num_of_hh_bed_nets, 0) AS num_of_hh_bed_nets, 
	COALESCE(family_survey.num_of_hh_latrines, 0) AS num_of_hh_latrines, 
	COALESCE(family_survey.num_of_hh_safe_water, 0) AS num_of_hh_safe_water,	
	COALESCE(health_forum.num_of_comm_members, 0) AS num_of_comm_members, 
	COALESCE(health_forum.num_of_health_forum, 0) AS num_of_health_forum,	
	COALESCE(num_pregnant.care, 0) AS num_pregnant_care,	
	COALESCE(num_pregnant.identified, 0) AS num_pregnant_identified,	
	COALESCE(deliveries.num_facility_deliveries, 0) AS num_facility_deliveries,	
	COALESCE(deliveries.num_home_deliveries, 0) AS num_home_deliveries,
	COALESCE(pregnancy.num_active_pregnant, 0) AS num_active_pregnant,
	COALESCE(pregnancy.num_anc_1, 0) AS num_anc_1,
	COALESCE(pregnancy.num_anc_2, 0) AS num_anc_2,
	COALESCE(pregnancy.num_anc_3, 0) AS num_anc_3,
	COALESCE(pregnancy.num_anc_4, 0) AS num_anc_4,	
	COALESCE(assess.tot_malaria_confirmed, 0) AS tot_malaria_confirmed,
	COALESCE(assess.num_malaria_u1_male, 0) AS num_malaria_u1_male,
	COALESCE(assess.num_malaria_u1_female, 0) AS num_malaria_u1_female,
	COALESCE(assess.num_malaria_u5_male, 0) AS num_malaria_u5_male,
	COALESCE(assess.num_malaria_u5_female, 0) AS num_malaria_u5_female,
	COALESCE(assess.num_diarrhea_u1_male, 0) AS num_diarrhea_u1_male,
	COALESCE(assess.num_diarrhea_u1_female, 0) AS num_diarrhea_u1_female,
	COALESCE(assess.num_diarrhea_u5_male, 0) AS num_diarrhea_u5_male,
	COALESCE(assess.num_diarrhea_u5_female, 0) AS num_diarrhea_u5_female,
	COALESCE(assess.num_ari_u1_male, 0) AS num_ari_u1_male,
	COALESCE(assess.num_ari_u1_female, 0) AS num_ari_u1_female,
	COALESCE(assess.num_ari_u5_male, 0) AS num_ari_u5_male,
	COALESCE(assess.num_ari_u5_female, 0) AS num_ari_u5_female,
	COALESCE(assess.tot_u5_assessed, 0) AS tot_u5_assessed,
	COALESCE(assess.tot_u1_assessed, 0) AS tot_u1_assessed,
	COALESCE(assess.malaria_treatment_u1_male, 0) AS malaria_treatment_u1_male,
	COALESCE(assess.malaria_treatment_u1_female, 0) AS malaria_treatment_u1_female,
	COALESCE(assess.u1_malaria_treatment, 0) AS u1_malaria_treatment,
	COALESCE(assess.malaria_treatment_u5_male, 0) AS malaria_treatment_u5_male,
	COALESCE(assess.malaria_treatment_u5_female, 0) AS malaria_treatment_u5_female,
	COALESCE(assess.u5_malaria_treatment, 0) AS u5_malaria_treatment,
	COALESCE(assess.malaria_treatment_over5_male, 0) AS malaria_treatment_over5_male,
	COALESCE(assess.malaria_treatment_over5_female, 0) AS malaria_treatment_over5_female,
	COALESCE(assess.over5_malaria_treatment, 0) AS over5_malaria_treatment,
	COALESCE(assess.diarrhea_treatment_u1_male, 0) AS diarrhea_treatment_u1_male,
	COALESCE(assess.diarrhea_treatment_u1_female, 0) AS diarrhea_treatment_u1_female,
	COALESCE(assess.u1_diarrhea_treatment, 0) AS u1_diarrhea_treatment,
	COALESCE(assess.diarrhea_treatment_u5_male, 0) AS diarrhea_treatment_u5_male,
	COALESCE(assess.diarrhea_treatment_u5_female, 0) AS diarrhea_treatment_u5_female,
	COALESCE(assess.u5_diarrhea_treatment, 0) AS u5_diarrhea_treatment,
	COALESCE(assess.ari_treatment_u1_male, 0) AS ari_treatment_u1_male,
	COALESCE(assess.ari_treatment_u1_female, 0) AS ari_treatment_u1_female,
	COALESCE(assess.u1_ari_treatment, 0) AS u1_ari_treatment,
	COALESCE(assess.ari_treatment_u5_male, 0) AS ari_treatment_u5_male,
	COALESCE(assess.ari_treatment_u5_female, 0) AS ari_treatment_u5_female,
	COALESCE(assess.u5_ari_treatment, 0) AS u5_ari_treatment,
	COALESCE(assess.num_treatments, 0) AS num_treatments,
	(
		COALESCE(assess.u1_malaria_treatment, 0) + 
		COALESCE(assess.u1_diarrhea_treatment, 0) + 
		COALESCE(assess.u1_ari_treatment, 0)
	) AS u1_num_treatments,
	(
		COALESCE(assess.u5_malaria_treatment, 0) + 
		COALESCE(assess.u5_diarrhea_treatment, 0) + 
		COALESCE(assess.u5_ari_treatment, 0)
	) AS u5_num_treatments,
	COALESCE(assess.tot_malaria_positive_u5, 0) AS tot_malaria_positive_u5,
	COALESCE(assess.tot_malaria_positive_over5, 0) AS tot_malaria_positive_over5,
	COALESCE(assess.tot_malaria_suspect_u5, 0) AS tot_malaria_suspect_u5,
	COALESCE(assess.tot_malaria_suspect_over5, 0) AS tot_malaria_suspect_over5,
	COALESCE(assess.num_reffered_malaria_u5_male, 0) AS num_reffered_malaria_u5_male,
	COALESCE(assess.num_reffered_malaria_u5_female, 0) AS num_reffered_malaria_u5_female,
	COALESCE(assess.num_reffered_malaria_over5_male, 0) AS num_reffered_malaria_over5_male,
	COALESCE(assess.num_reffered_malaria_over5_female, 0) AS num_reffered_malaria_over5_female,
	COALESCE(assess.num_reffered_diarrhea_u5_male, 0) AS num_reffered_diarrhea_u5_male,
	COALESCE(assess.num_reffered_diarrhea_u5_female, 0) AS num_reffered_diarrhea_u5_female,
	COALESCE(assess.num_reffered_ari_u5_male, 0) AS num_reffered_ari_u5_male,
	COALESCE(assess.num_reffered_ari_u5_female, 0) AS num_reffered_ari_u5_female,	
	COALESCE(follow_Up.num_accute_u5_malaria, 0) AS num_accute_u5_malaria,	
	COALESCE(follow_Up.num_accute_u5_diarrhea, 0) AS num_accute_u5_diarrhea,	
	COALESCE(follow_Up.num_accute_u5_ari, 0) AS num_accute_u5_ari,	
	COALESCE(follow_Up.num_u5_reffered_followed, 0) AS num_u5_reffered_followed,	
	COALESCE(follow_Up.num_u5_followed_malaria, 0) AS num_u5_followed_malaria,	
	COALESCE(follow_Up.num_u5_followed_diarrhea, 0) AS num_u5_followed_diarrhea,	
	COALESCE(follow_Up.num_u5_followed_ari, 0) AS num_u5_followed_ari,	
	COALESCE(follow_Up.num_u5_treated, 0) AS num_u5_treated,	
	COALESCE(demography.num_eligible_women, 0) AS num_eligible_women,
	COALESCE(cumulative_demography.tot_eligible_women, 0) AS tot_eligible_women,
	COALESCE(cumulative_demography.tot_eligible_women_old, 0) AS tot_eligible_women_old,
	COALESCE(demography.num_u5_children, 0) AS num_u5_children,
	COALESCE(immunization.num_u5_imm, 0) AS num_u5_imm,
	COALESCE(FPREFERRALS.long_term_fp_referrals, 0) AS long_term_fp_referrals,
	COALESCE(deaths.u5_count, 0) AS num_u5_deaths,
	COALESCE(demography.num_hh_with_u5, 0) AS num_hh_with_u5,	
	COALESCE(pnc.pnc_visit_48_hrs, 0) AS pnc_visit_48_hrs,		
	COALESCE(visit.num_pregnant_immunized, 0) AS num_pregnant_immunized,	
	COALESCE(visit.num_anc_at_facility, 0) AS num_anc_at_facility
	
	
FROM
	(
		SELECT
			chp.area_uuid,
			chp.uuid,
			chp.name,
			chp.branch_name,
			chp.branch_uuid,
			chp.supervisor_uuid,
			chp.region,	
			generate_series(date_trunc('month',('{{ var("start_date") }}')::date), 
							date_trunc('month',('{{ var("end_date") }}')::date), 
							'1 month'::interval
							)::DATE AS date
		FROM
			{{ ref("contactview_chp") }} AS chp
	) AS period_chp

LEFT JOIN
(
	SELECT
		meta.parent_uuid as area_uuid,
		COUNT(person.uuid) FILTER(
			WHERE extract (YEAR from age(now()::date, to_date(person.date_of_birth,'YYYY-MM-DD')))::int BETWEEN 12 AND 52  
			AND person.sex ='female' AND (date_trunc('hour',(person.reported)::timestamp))::date <= date_trunc('hour',('{{ var("end_date") }}')::date)::date
		) AS tot_eligible_women,
		COUNT(person.uuid) FILTER(
			WHERE extract (YEAR from age(now()::date, to_date(person.date_of_birth,'YYYY-MM-DD')))::int BETWEEN 12 AND 52  
			AND person.sex ='female' AND (date_trunc('hour',(person.reported)::timestamp))::date <= (date_trunc('hour',('{{ var("end_date") }}')::date)::date - INTERVAL '1 months')
		) AS tot_eligible_women_old,
		COUNT(DISTINCT person.parent_uuid) AS total_households
	FROM 
		{{ ref("contactview_person") }} person
	LEFT JOIN {{ ref("contactview_metadata") }} AS meta ON person.parent_uuid = meta.uuid
	WHERE
		meta.type = 'clinic'
	GROUP BY 
		meta.parent_uuid
) cumulative_demography ON (period_chp.area_uuid = cumulative_demography.area_uuid)
	
LEFT JOIN
	(
		SELECT
			parent_uuid AS area_uuid,
			date_trunc('month',reported::timestamp) AS reported_month,
			sum(1) AS count
			
			
		FROM
			{{ ref("contactview_metadata") }}
			
		WHERE
			type = 'clinic'
			
		GROUP BY
			area_uuid,
			reported_month
				
	) AS num_of_hh_covered ON (period_chp.date = num_of_hh_covered.reported_month AND period_chp.area_uuid = num_of_hh_covered.area_uuid)
	
LEFT JOIN
	(
		SELECT
			fmeta.reported_by_parent AS area_uuid,
			date_trunc('month',fmeta.reported::timestamp) AS reported_month,
			COUNT(DISTINCT(cperson.parent_uuid)) AS count
			
		FROM
			{{ ref("form_metadata") }} AS fmeta
		INNER JOIN contactview_person AS cperson ON (fmeta.patient_id = cperson.patient_id)
		WHERE
			fmeta.reported BETWEEN '{{ var("start_date") }}' AND '{{ var("end_date") }}' /* only pick whats necessary */
			
		GROUP BY
			area_uuid,
			reported_month
	
	) AS num_of_hh_visited ON (period_chp.date = num_of_hh_visited.reported_month AND period_chp.area_uuid = num_of_hh_visited.area_uuid)
LEFT JOIN 
(

	SELECT  
		chw, 
		area_uuid, 	
		date_trunc('month',reported::timestamp) :: date  AS reported_month, 
		SUM((mosquito_nets ='true')::int) AS num_of_hh_bed_nets, 
    	SUM((hygeinic_toilet ='true')::int) AS num_of_hh_latrines, 
		SUM((COALESCE(source_of_drinking_water,'') != 'spring')::int) AS num_of_hh_safe_water
	FROM {{ ref("useview_household_survey") }}

	GROUP BY 
		chw, 
		area_uuid, 
		reported_month 


) AS family_survey ON(period_chp.date = family_survey.reported_month  AND  period_chp.area_uuid =family_survey.area_uuid)
LEFT JOIN
(

	SELECT 
		chw, 
		area_uuid,  
		SUM( 
		CASE 
		WHEN CHARACTER_LENGTH(no_of_participants) > 4 THEN 0  /* guard against invalid entry from users*/
		ELSE  no_of_participants ::int
		END ) AS num_of_comm_members, 
		COUNT(xmlforms_uuid) AS num_of_health_forum,
		date_trunc('month',reported_day::date) AS reported_month
	FROM {{ ref("useview_health_forum") }}
	GROUP BY 
 		chw, 
 		area_uuid,
 		reported_month

) AS health_forum ON (period_chp.date = health_forum. reported_month AND period_chp.area_uuid = health_forum.area_uuid )
LEFT JOIN 
(
	WITH preg_record AS (
		SELECT 
			chp.area_uuid,
			date_trunc('MONTH', reported::timestamp) AS reported_month,
			COUNT(DISTINCT patient_id) FILTER(WHERE preg_test != 'neg') AS count
		FROM {{ ref("useview_pregnancy") }} preg
		LEFT JOIN contactview_chp chp ON chp.uuid =  preg.chw 
		WHERE date_trunc('month',reported::timestamp)::DATE <= date_trunc('MONTH',('{{ var("end_date") }}')::DATE)
		GROUP BY 
			area_uuid, 
			reported_month
	)
	SELECT
		area_uuid,
		reported_month,
		count as identified, /* Count those registered this month*/
		SUM(count) OVER (PARTITION BY area_uuid ORDER BY reported_month) AS care /* Count all those registered in the past*/
	FROM {{ ref("preg_record") }}
			
	)	AS num_pregnant ON (num_pregnant.reported_month = period_chp.date  AND period_chp.area_uuid = num_pregnant.area_uuid)
	LEFT JOIN 
	(
	SELECT  DISTINCT
		fpostnatal.reported_by_parent AS area_uuid,
		SUM ((fpostnatal.health_facility_delivery ='yes' )::int  ) AS num_facility_deliveries,
		SUM ((fpostnatal.health_facility_delivery ='no')::int ) AS num_home_deliveries,  /* Assumption is that home deliveries have answer no  */
		date_trunc('month',(fpostnatal.reported::timestamp)::date)  AS reported_month
	FROM 
		{{ ref("useview_postnatal_care") }} AS fpostnatal
	WHERE 
		(date_trunc('month',fpostnatal.reported::timestamp)::DATE) >= (date_trunc('MONTH',('{{ var("start_date") }}')::DATE)) AND (date_trunc('month',fpostnatal.reported) ::DATE) <= (date_trunc('MONTH',('{{ var("end_date") }}')::DATE))
	GROUP BY
		area_uuid,
		reported_month
	
	
	) AS deliveries ON (deliveries.area_uuid = period_chp.area_uuid  AND deliveries.reported_month = period_chp.date ) 
  LEFT JOIN
  (
  	SELECT  
		chw, 
		date_trunc('month', reported)  AS reported_month, 
		SUM((edd > date_trunc('month', reported))::int) AS num_active_pregnant,
		SUM((anc_visit >= 1)::int) AS num_anc_1,
		SUM((anc_visit >= 2)::int) AS num_anc_2,
		SUM((anc_visit >= 3)::int) AS num_anc_3,
		SUM((anc_visit >= 4)::int) AS num_anc_4
	FROM 
		{{ ref("useview_pregnancy") }}
		
	WHERE 
		(date_trunc('month',useview_pregnancy.reported) ::DATE) >= (date_trunc('MONTH',('{{ var("start_date") }}')::DATE)) AND (date_trunc('month',useview_pregnancy.reported) ::DATE) <= (date_trunc('MONTH',('{{ var("end_date") }}')::DATE))
	

	GROUP BY 
		chw,
		reported_month
		
  ) AS pregnancy ON (pregnancy.chw = period_chp.uuid AND pregnancy.reported_month = period_chp.date)
  LEFT JOIN 
  (
  SELECT 
	
	reported_by_parent  AS  area_uuid, 
	date_trunc ('MONTH',reported) AS reported_month, 
	SUM ((COALESCE(mrdt_result,'')= 'positive')::int) AS tot_malaria_confirmed,	
	SUM ((COALESCE(mrdt_result,'')= 'positive'  AND COALESCE (patient_age_in_months,0) >2 AND COALESCE (patient_age_in_months,0) <12  AND COALESCE(sex,'') ='male')::int) AS num_malaria_u1_male,
	SUM ((COALESCE(mrdt_result,'')= 'positive'  AND COALESCE (patient_age_in_months,0) >2 AND COALESCE (patient_age_in_months,0) <12  AND COALESCE(sex,'') ='female')::int) AS num_malaria_u1_female,
	SUM ((COALESCE(mrdt_result,'')= 'positive'  AND COALESCE (patient_age_in_months,0) >2 AND COALESCE (patient_age_in_months,0) <60 AND COALESCE(sex,'') ='male')::int) AS num_malaria_u5_male,
	SUM ((COALESCE(mrdt_result,'')= 'positive'  AND COALESCE (patient_age_in_months,0) >2 AND COALESCE (patient_age_in_months,0) <60  AND COALESCE(sex,'') ='female')::int) AS num_malaria_u5_female,
	SUM ((COALESCE(diagnosis_cough,'')   ~* 'pneumonia1|pneumonia2b|pneumonia2c'  AND COALESCE (patient_age_in_months,0) >2 AND COALESCE (patient_age_in_months,0) <12  AND COALESCE(sex,'') ='male')::int) AS num_ari_u1_male,
	SUM ((COALESCE(diagnosis_cough,'')   ~* 'pneumonia1|pneumonia2b|pneumonia2c'  AND COALESCE (patient_age_in_months,0) >2 AND COALESCE (patient_age_in_months,0) <12  AND COALESCE(sex,'') ='female')::int) AS num_ari_u1_female,
	SUM ((COALESCE(diagnosis_cough,'')   ~* 'pneumonia1|pneumonia2b|pneumonia2c'  AND COALESCE (patient_age_in_months,0) >2 AND COALESCE (patient_age_in_months,0) <60  AND COALESCE(sex,'') ='male')::int) AS num_ari_u5_male,
	SUM ((COALESCE(diagnosis_cough,'')   ~* 'pneumonia1|pneumonia2b|pneumonia2c'  AND COALESCE (patient_age_in_months,0) >2 AND COALESCE (patient_age_in_months,0) <60  AND COALESCE(sex,'') ='female')::int) AS num_ari_u5_female,
	SUM ((COALESCE(diagnosis_diarrhea,'')   ~* 'diarrhea2|diarrhea1'  AND COALESCE (patient_age_in_months,0) >2 AND COALESCE (patient_age_in_months,0) <12  AND COALESCE(sex,'') ='male')::int) AS num_diarrhea_u1_male,
	SUM ((COALESCE(diagnosis_diarrhea,'')   ~* 'diarrhea2|diarrhea1'  AND COALESCE (patient_age_in_months,0) >2 AND COALESCE (patient_age_in_months,0) <12  AND COALESCE(sex,'') ='female')::int) AS num_diarrhea_u1_female,
	SUM ((COALESCE(diagnosis_diarrhea,'')   ~* 'diarrhea2|diarrhea1'  AND COALESCE (patient_age_in_months,0) >2 AND COALESCE (patient_age_in_months,0) <60  AND COALESCE(sex,'') ='male')::int) AS num_diarrhea_u5_male,
	SUM ((COALESCE(diagnosis_diarrhea,'')   ~* 'diarrhea2|diarrhea1'  AND COALESCE (patient_age_in_months,0) >2 AND COALESCE (patient_age_in_months,0) <60  AND COALESCE(sex,'') ='female')::int) AS num_diarrhea_u5_female,
	COUNT(uuid) FILTER(WHERE patient_age_in_months < 60) AS tot_u5_assessed,
	COUNT(uuid) FILTER(WHERE patient_age_in_months < 12) AS tot_u1_assessed,
	SUM ((malaria_treatment IS NOT NULL)::int + (diarrhea_treatment IS NOT NULL)::int + (pneumonia_treatment IS NOT NULL)::int) AS num_treatments,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 1 AND malaria_treatment IS NOT NULL AND COALESCE(sex,'') ='male') AS malaria_treatment_u1_male,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 1 AND malaria_treatment IS NOT NULL AND COALESCE(sex,'') ='female') AS malaria_treatment_u1_female,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 1 AND malaria_treatment IS NOT NULL) AS u1_malaria_treatment,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 5 AND malaria_treatment IS NOT NULL AND COALESCE(sex,'') ='male') AS malaria_treatment_u5_male,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 5 AND malaria_treatment IS NOT NULL AND COALESCE(sex,'') ='female') AS malaria_treatment_u5_female,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 5 AND malaria_treatment IS NOT NULL) AS u5_malaria_treatment,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int >= 5 AND malaria_treatment IS NOT NULL AND COALESCE(sex,'') ='male') AS malaria_treatment_over5_male,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int >= 5 AND malaria_treatment IS NOT NULL AND COALESCE(sex,'') ='female') AS malaria_treatment_over5_female,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int >= 5 AND malaria_treatment IS NOT NULL) AS over5_malaria_treatment,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 1 AND diarrhea_treatment IS NOT NULL AND COALESCE(sex,'') ='male') AS diarrhea_treatment_u1_male,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 1 AND diarrhea_treatment IS NOT NULL AND COALESCE(sex,'') ='female') AS diarrhea_treatment_u1_female,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 1 AND diarrhea_treatment IS NOT NULL) AS u1_diarrhea_treatment,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 5 AND diarrhea_treatment IS NOT NULL AND COALESCE(sex,'') ='male') AS diarrhea_treatment_u5_male,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 5 AND diarrhea_treatment IS NOT NULL AND COALESCE(sex,'') ='female') AS diarrhea_treatment_u5_female,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 5 AND diarrhea_treatment IS NOT NULL) AS u5_diarrhea_treatment,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 1 AND pneumonia_treatment IS NOT NULL AND COALESCE(sex,'') ='male') AS ari_treatment_u1_male,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 1 AND pneumonia_treatment IS NOT NULL AND COALESCE(sex,'') ='female') AS ari_treatment_u1_female,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 1 AND pneumonia_treatment IS NOT NULL) AS u1_ari_treatment,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 5 AND pneumonia_treatment IS NOT NULL AND COALESCE(sex,'') ='male') AS ari_treatment_u5_male,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 5 AND pneumonia_treatment IS NOT NULL AND COALESCE(sex,'') ='female') AS ari_treatment_u5_female,
	COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 5 AND pneumonia_treatment IS NOT NULL) AS u5_ari_treatment,
	SUM ((COALESCE(mrdt_result,'')= 'positive'  AND COALESCE (patient_age_in_months,0) < 60)::int) AS tot_malaria_positive_u5,
	SUM ((COALESCE(mrdt_result,'')= 'positive'  AND COALESCE (patient_age_in_months,0) >= 60)::int) AS tot_malaria_positive_over5,	
	SUM ((COALESCE(mrdt_result,'')   ~* 'positive|negative'  AND COALESCE (patient_age_in_months,0) < 60)::int) AS tot_malaria_suspect_u5,
	SUM ((COALESCE(mrdt_result,'')   ~* 'positive|negative'  AND COALESCE (patient_age_in_months,0) >= 60)::int) AS tot_malaria_suspect_over5,
	SUM ((COALESCE(diagnosis_fever,'')   ~*  'malaria2|malaria1'  AND COALESCE (patient_age_in_months,0) <4  AND COALESCE(sex,'') ='male')::int) AS num_reffered_malaria_u5_male,/*All patients under 4 months are refferred*/
	SUM ((COALESCE(diagnosis_fever,'')   ~*  'malaria2|malaria1'  AND COALESCE (patient_age_in_months,0) <4 AND COALESCE(sex,'') ='female')::int) AS num_reffered_malaria_u5_female,
	SUM ((COALESCE(diagnosis_fever,'')   ~*  'malaria2|malaria1'  AND COALESCE (patient_age_in_months,0) >= 60  AND COALESCE(sex,'') ='male')::int) AS num_reffered_malaria_over5_male,
	SUM ((COALESCE(diagnosis_fever,'')   ~*  'malaria2|malaria1'  AND COALESCE (patient_age_in_months,0) >= 60 AND COALESCE(sex,'') ='female')::int) AS num_reffered_malaria_over5_female,
	SUM ((COALESCE(diagnosis_diarrhea,'')   ~* 'diarrhea2|diarrhea1'  AND COALESCE (patient_age_in_months,0) < 60 AND COALESCE(referral_follow_up,'')='true' AND COALESCE(sex,'') ='male')::int) AS num_reffered_diarrhea_u5_male,
	SUM ((COALESCE(diagnosis_diarrhea,'')   ~* 'diarrhea2|diarrhea1'  AND COALESCE (patient_age_in_months,0) < 60 AND COALESCE(referral_follow_up,'')='true' AND COALESCE(sex,'') ='female')::int) AS num_reffered_diarrhea_u5_female,
	SUM ((COALESCE(diagnosis_cough,'')   ~* 'pneumonia1|pneumonia2b|pneumonia2c'  AND COALESCE (patient_age_in_months,0) <60 AND COALESCE(referral_follow_up,'')='true'  AND COALESCE(sex,'') ='male')::int) AS num_reffered_ari_u5_male,
	SUM ((COALESCE(diagnosis_cough,'')   ~* 'pneumonia1|pneumonia2b|pneumonia2c'  AND COALESCE (patient_age_in_months,0) <60 AND COALESCE(referral_follow_up,'')='true' AND COALESCE(sex,'') ='female')::int) AS num_reffered_ari_u5_female
	
FROM {{ ref("useview_assessment") }}

WHERE 
		(date_trunc('month',useview_assessment.reported) ::DATE) >= (date_trunc('MONTH',('{{ var("start_date") }}')::DATE)) AND (date_trunc('month',useview_assessment.reported) ::DATE) <= (date_trunc('MONTH',('{{ var("end_date") }}')::DATE))
	
GROUP BY 
area_uuid,
reported_month
  
  ) AS assess ON ( assess. area_uuid = period_chp.area_uuid AND assess.reported_month = period_chp.date )

 LEFT JOIN
 (
 SELECT 
	date_trunc('MONTH',assess.reported) reported_month, 
	assess.reported_by_parent AS  area_uuid,
	SUM ((COALESCE(assess.diagnosis_fever,'')   ~*  'malaria2|malaria1'  AND COALESCE (assess.patient_age_in_months,0) <60 AND COALESCE(assess.referral_follow_up,'') ~* 'true')::int) AS num_accute_u5_malaria,
	SUM ((COALESCE(assess.diagnosis_diarrhea,'')   ~* 'diarrhea2|diarrhea1'  AND COALESCE (assess.patient_age_in_months,0) <60 AND COALESCE(assess.referral_follow_up,'') ~* 'true')::int) AS num_accute_u5_diarrhea,
	SUM ((COALESCE(assess.diagnosis_cough,'')   ~* 'pneumonia1|pneumonia2b|pneumonia2c'  AND COALESCE (assess.patient_age_in_months,0) <60 AND COALESCE(assess.referral_follow_up,'') ~* 'true')::int) AS num_accute_u5_ari,
	SUM ((COALESCE(assess.patient_age_in_months,0) < 60 AND COALESCE(assess.referral_follow_up,'') ~* 'true' )::int) AS num_u5_reffered_followed,
	SUM ((COALESCE(assess.diagnosis_fever,'')  ~* 'malaria2|malaria1'  AND COALESCE (assess.patient_age_in_months,0) <60  AND COALESCE(assess.treatment_follow_up,'')  ~* 'true' AND assess.reported >= follow_up.reported - interval '48 hours')::int) AS num_u5_followed_malaria,
	SUM ((COALESCE(assess.diagnosis_diarrhea,'')   ~* 'diarrhea2|diarrhea1'  AND COALESCE (assess.patient_age_in_months,0) <60 AND COALESCE(assess.treatment_follow_up,'')  ~* 'true'  AND assess.reported >= follow_up.reported - interval '48 hours')::int) AS num_u5_followed_diarrhea,
	SUM ((COALESCE(assess.diagnosis_cough,'')   ~* 'pneumonia1|pneumonia2b|pneumonia2c'  AND COALESCE (assess.patient_age_in_months,0) <60 AND COALESCE(assess.treatment_follow_up,'')  ~* 'true'  AND assess.reported >= follow_up.reported - interval '48 hours')::int) AS num_u5_followed_ari,
	SUM ((COALESCE(assess.patient_age_in_months,0) < 60 AND COALESCE(assess.treatment_follow_up,'')  ~* 'true'   AND assess.reported >= follow_up.reported - interval '48 hours')::int) AS num_u5_treated

FROM {{ ref("useview_assessment") }} AS assess

INNER JOIN {{ ref("useview_assessment_follow_up") }}  AS follow_up 
ON  assess.uuid = follow_up.form_source_id

WHERE 
		(date_trunc('month',assess.reported) ::DATE) >= (date_trunc('MONTH',('{{ var("start_date") }}')::DATE)) AND (date_trunc('month',assess.reported) ::DATE) <= (date_trunc('MONTH',('{{ var("end_date") }}')::DATE))

	
GROUP BY 
	reported_month, 
	area_uuid
 
 
 ) AS follow_Up ON ( follow_Up. area_uuid = period_chp.area_uuid AND follow_Up.reported_month = period_chp.date )

 LEFT JOIN (
	WITH immunization_followup 
	AS	(
			SELECT DISTINCT ON (patient_id)
				"inputs/source_id",
				vaccines_administered 
			FROM {{ ref("formview_immunization_follow_up") }}
			WHERE 
			(date_trunc('month', reported)::DATE) <= (date_trunc('MONTH',('{{ var("end_date") }}')::DATE)) 
			ORDER BY patient_id, reported DESC
		)
	SELECT
		date_trunc('month', imm.reported) reported_month,
		imm.reported_by_parent AS  area_uuid,
		COUNT(
			NULLIF(COALESCE(imm_given_2mo, imm_given_9mo, imm_given_18mo, fu.vaccines_administered), '')
		) as num_u5_imm
	FROM {{ ref("useview_assessment") }} imm
	LEFT JOIN immunization_followup fu ON "inputs/source_id" = imm.uuid
	WHERE 
		patient_age_in_months < 60
		AND (date_trunc('month', imm.reported)::DATE) >= (date_trunc('month',('{{ var("start_date") }}')::DATE)) 
		AND (date_trunc('month', imm.reported)::DATE) <= (date_trunc('MONTH',('{{ var("end_date") }}')::DATE))
	GROUP BY 
		area_uuid, 
		reported_month
 ) immunization ON ( immunization.area_uuid = period_chp.area_uuid AND immunization.reported_month = period_chp.date )

 LEFT JOIN
 (
	SELECT
	 	date_trunc('month', reported) reported_month,
		reported_by_parent AS  area_uuid,
	 	COUNT(uuid) FILTER(WHERE referred_for_fp_method IS TRUE) AS long_term_fp_referrals
	FROM {{ ref("fp_referral_cases") }}
	WHERE
		(date_trunc('month', reported)::DATE) >= (date_trunc('month',('{{ var("start_date") }}')::DATE)) 
		AND (date_trunc('month', reported)::DATE) <= (date_trunc('MONTH',('{{ var("end_date") }}')::DATE))
	GROUP BY 
		area_uuid, 
		reported_month
 ) FPREFERRALS ON ( FPREFERRALS.area_uuid = period_chp.area_uuid AND FPREFERRALS.reported_month = period_chp.date )

 LEFT JOIN
 (
	 SELECT
	 	date_trunc('month', reported) reported_month,
		reported_by_parent AS  area_uuid,
		COUNT(uuid) as COUNT,
		COUNT(uuid) FILTER(WHERE patient_age_in_years < 5) AS u5_count
	FROM {{ ref("formview_death_confirmation") }}
	WHERE
		(date_trunc('month', reported)::DATE) >= (date_trunc('month',('{{ var("start_date") }}')::DATE)) 
		AND (date_trunc('month', reported)::DATE) <= (date_trunc('MONTH',('{{ var("end_date") }}')::DATE))
	GROUP BY 
		area_uuid, 
		reported_month
 ) deaths ON (deaths.area_uuid = period_chp.area_uuid AND deaths.reported_month = period_chp.date)
 
 LEFT JOIN (
 
 	SELECT 
		SUM ((extract (YEAR from age(now()::date, to_date(person.date_of_birth,'YYYY-MM-DD')))::int >=12 AND extract (YEAR from age(now()::date, to_date(person.date_of_birth,'YYYY-MM-DD')))::int <=52  AND COALESCE(person.sex,'') ='female')::int) AS num_eligible_women,
		SUM ((extract (YEAR from age(now()::date, to_date(person.date_of_birth,'YYYY-MM-DD')))::int < 5 )::int) AS num_u5_children,
		COUNT( DISTINCT(person.parent_uuid)) FILTER (WHERE extract (YEAR from age(now()::date, to_date(person.date_of_birth,'YYYY-MM-DD')))::int < 5) as num_hh_with_u5,
		date_trunc('month',person.reported) AS reported_month,
		meta.PARENT_UUID AS area_uuid
	FROM {{ ref("contactview_person") }} person
	LEFT JOIN {{ ref("contactview_metadata") }} AS meta 
	ON person.parent_uuid = meta.UUID
	
	WHERE 
		(date_trunc('month',person.reported) ::DATE) >= (date_trunc('MONTH',('{{ var("start_date") }}')::DATE)) AND (date_trunc('month',person.reported) ::DATE) <= (date_trunc('MONTH',('{{ var("end_date") }}')::DATE))


	GROUP BY 
		area_uuid, 
		reported_month 
 ) AS  demography ON  ( demography. area_uuid = period_chp.area_uuid AND demography.reported_month = period_chp.date )
LEFT JOIN
(

SELECT 
	reported_by_parent AS area_uuid,
	date_trunc('month',delivery_date)::date AS reported_month,
	SUM((first_visit_on_time)::int) AS pnc_visit_48_hrs
FROM {{ ref("pncview_actual_enrollments") }}
WHERE 
		(date_trunc('month',delivery_date) ::DATE) >= (date_trunc('MONTH',('{{ var("start_date") }}')::DATE)) AND (date_trunc('month',delivery_date) ::DATE) <= (date_trunc('MONTH',('{{ var("end_date") }}')::DATE))


GROUP BY 
area_uuid,
reported_month

)AS  pnc ON  ( pnc. area_uuid = period_chp.area_uuid AND pnc.reported_month = period_chp.date )
LEFT JOIN 
(
	SELECT 
		reported_month, 
		area_uuid, 
		SUM (((CASE 
    			WHEN tt ='' THEN 0
    			ELSE RIGHT(tt,1)::int END
    			)>0)::int) AS num_pregnant_immunized , 
		SUM (((CASE 
    			WHEN anc_visit ='' THEN 0
    			ELSE RIGHT(anc_visit,1)::int END
    			) > 0)::int ) AS num_anc_at_facility
	FROM {{ ref("useview_pregnancy_visit") }}
    WHERE 
	reported_month::DATE >= (date_trunc('MONTH',('{{ var("start_date") }}')::DATE)) AND (reported_month ::DATE) <= (date_trunc('MONTH',('{{ var("end_date") }}')::DATE))

	GROUP BY 
 		reported_month, 
 		area_uuid
) AS visit ON ( visit. area_uuid = period_chp.area_uuid AND visit.reported_month = period_chp.date )