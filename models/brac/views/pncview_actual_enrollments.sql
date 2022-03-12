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
	{{ ref("confirmed_deliveries_CTE") }} AS deliv