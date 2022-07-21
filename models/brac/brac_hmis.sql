SELECT
	period_chp.uuid,
	period_chp.name,
	period_chp.date,
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
	COALESCE(deliveries.num_home_deliveries, 0) AS num_home_deliveries

FROM
	(
		SELECT
			chp.area_uuid,
			chp.uuid,
			chp.name,
			date_trunc('month',now() - '1 month'::INTERVAL)::date AS date
		FROM
			contactview_chp AS chp
	
	) AS period_chp
	
LEFT JOIN
	(
		SELECT
			parent_uuid AS area_uuid,
			date_trunc('month',reported) AS reported_month,
			sum(1) AS count
			
		FROM
			contactview_metadata
			
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
			date_trunc('month',fmeta.reported) AS reported_month,
			COUNT(DISTINCT(cperson.parent_uuid)) AS count
			
		FROM
			form_metadata AS fmeta
			INNER JOIN contactview_person AS cperson ON (fmeta.patient_id = cperson.patient_id)
			
		GROUP BY
			area_uuid,
			reported_month
	
	) AS num_of_hh_visited ON (period_chp.date = num_of_hh_visited.reported_month AND period_chp.area_uuid = num_of_hh_visited.area_uuid)
LEFT JOIN 
(

	SELECT  
		chw, 
		area_uuid, 	
		date_trunc('month',reported) :: date  AS reported_month, 
		SUM((mosquito_nets ='true')::int) AS num_of_hh_bed_nets, 
    	SUM((hygeinic_toilet ='true')::int) AS num_of_hh_latrines, 
		SUM((source_of_drinking_water !='spring')::int) AS num_of_hh_safe_water
	FROM public.useview_household_survey

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
		SUM(no_of_participants::bigint) AS num_of_comm_members, 
		COUNT(xmlforms_uuid) AS num_of_health_forum,
		date_trunc('month',reported_day	) AS reported_month
	FROM public.formview_health_forum
	GROUP BY 
 		chw, 
 		area_uuid,
 		reported_month

) AS health_forum ON (period_chp.date = health_forum. reported_month AND period_chp.area_uuid = health_forum.area_uuid )

LEFT JOIN 
(

		SELECT
			fmeta.reported_by_parent AS area_uuid,
			SUM((fmeta.reported < (date_trunc('MONTH',now() - '1 month'::INTERVAL)::DATE))::int) AS care,
			SUM((fmeta.reported >= (date_trunc('MONTH',now()::date - '1 month'::INTERVAL)::DATE))::int) AS identified
			
		FROM
			form_metadata AS fmeta
		WHERE 
			fmeta.form='pregnancy'  
			
		GROUP BY
			area_uuid

) AS num_pregnant ON (period_chp.area_uuid = num_pregnant.area_uuid)
LEFT JOIN 
(
	SELECT 
		fpostnatal.reported_by_parent AS area_uuid,
		SUM ((fpostnatal.health_facility_delivery ='yes')::int ) AS num_facility_deliveries,
		SUM ((fpostnatal.health_facility_delivery ='no')::int ) AS num_home_deliveries,  /* Assumption is that home deliveries have answer no  */
		date_trunc('month',fpostnatal.reported)::date  AS reported_month
	FROM 
		public.useview_postnatal_care AS fpostnatal
	WHERE 
		(date_trunc('month',fpostnatal.reported) ::DATE) = (date_trunc('MONTH',now()::date - '1 month'::INTERVAL)::DATE)
	GROUP BY
		area_uuid,
		reported_month
) AS deliveries ON (deliveries.area_uuid = period_chp.area_uuid )