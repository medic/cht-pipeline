WITH confirmed_deliveries_CTE AS (
	SELECT
	DISTINCT ON (deliv.patient_id)
	deliv.uuid AS delivery_id,
	deliv.patient_id AS patient_id,
	deliv.reported_by AS reported_by,
	deliv.reported_by_parent AS reported_by_parent,
	CASE
		WHEN deliv.delivery_date = '' THEN NULL
		ELSE deliv.delivery_date 
	END AS delivery_date,
	deliv.reported AS delivery_form_submission,
	deliv.baby_danger_signs <> '' AS danger_sign_at_deliv,
	deliv.pregnancy_outcome AS pregnancy_outcome,
	deliv.follow_up_count AS follow_up_count,
	deliv.health_facility_delivery = 'yes' AS facility_delivery,
	deliv.reported AS first_pnc_visit_date,
	deliv.reported AS first_pnc_form_submission,
	deliv.follow_up_method AS follow_up_method
	
FROM
	{{ ref("useview_postnatal_care") }} AS deliv

WHERE
	deliv.patient_id IS NOT NULL
	AND deliv.patient_id <> ''
	AND deliv.follow_up_count = '1'
	AND (deliv.pregnancy_outcome = 'healthy' OR deliv.pregnancy_outcome = 'still_birth' OR deliv.pregnancy_outcome = '' OR deliv.pregnancy_outcome IS NULL)

ORDER BY
	deliv.patient_id,
	deliv.reported asc
)

SELECT
	delivery_id,
	patient_id,
	reported_by,
	reported_by_parent,
	delivery_date::date,
	danger_sign_at_deliv,
	facility_delivery,
	CASE
		WHEN facility_delivery THEN TRUE
		WHEN NOT facility_delivery 
			AND follow_up_method = 'in_person' 
			AND (first_pnc_visit_date::date - delivery_date::date)::int <= 3::int THEN TRUE 
		ELSE FALSE
	END AS first_visit_on_time,
	delivery_form_submission,
	first_pnc_form_submission
		
FROM
	confirmed_deliveries_CTE AS deliv