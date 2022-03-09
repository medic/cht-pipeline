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