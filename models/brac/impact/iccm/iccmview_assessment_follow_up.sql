SELECT
	fu.uuid AS uuid,
	fu.form AS form,
	fu.patient_id AS patient_id,
	fu.reported_by AS reported_by,
	fu.reported_by_parent AS reported_by_parent,
	fu.reported AS reported,
	fu.form_source_id AS source_id,
	source.form AS source_type,
	CASE
		--WHEN source.form = 'assessment_cbds' THEN fu.form_source_id
		WHEN source.form = 'assessment' THEN fu.form_source_id
		ELSE NULL
	END AS original_assessment_id,
	(fu.follow_up_type = 'refer_only' OR fu.follow_up_type = 'treat_refer') AS fu_ref,
	--fu.follow_up_type ~~ 'refer%' AS fu_ref,
	--fu.follow_up_type ~~ 'treat%' AS fu_tx,

	(fu.patient_improved = 'yes' OR fu.patient_better = 'cured' OR fu.patient_better = 'still_recovering') AS patient_condition_improved,
	CASE
		WHEN fu.patient_improved <> '' OR fu.patient_better <> '' THEN true
		ELSE FALSE
	END AS patient_condition_reported,
	fu.follow_up_method = 'in_person' AS in_person,
	fu.patient_health_facility_visit = 'yes' AS facility_attended,
	(fu.follow_up_type = 'treat' OR fu.follow_up_type = 'treat_refer') AS treatment_confirmed, /* is that really what this means? */
	fu.referral_follow_up_needed = 'true' AS new_fu_ref_rec,
	
	fu.follow_up_count AS follow_up_count,
	fu.danger_signs <> '' AS danger_sign
	

FROM
	{{ ref("useview_assessment_follow_up") }}  AS fu
	/* restrict so that it is only coming from symptomatic assessments */
	INNER JOIN {{ ref("iccmview_assessment") }} AS source ON (fu.form_source_id = source.uuid)
	
WHERE
	/* restrict patient age */
	fu.patient_age_in_months >= 2 
	AND fu.patient_age_in_months < 60