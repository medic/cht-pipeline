SELECT
	assess.uuid AS uuid,
	assess.form AS form,
	assess.patient_id AS patient_id,
	assess.reported_by AS reported_by,
	assess.reported_by_parent, 															
	assess.reported,
	assess.danger_signs <> '' AS danger_sign,	     	
    (assess.diagnosis_fever ~~ 'malaria%') AS malaria_dx,
	(assess.diagnosis_diarrhea ~~ 'diarrhea%') AS diarrhea_dx,
	(assess.diagnosis_cough ~~ 'pneumonia%') AS pneumonia_dx,
	
	
	(greatest(assess.fever_duration,assess.diarrhea_duration,assess.coughing_duration) = 1) AS within_24,
	(greatest(assess.fever_duration,assess.diarrhea_duration,assess.coughing_duration) = 2) AS within_25_to_48,
	(greatest(assess.fever_duration,assess.diarrhea_duration,assess.coughing_duration) = 3) AS within_49_to_72,
	(greatest(assess.fever_duration,assess.diarrhea_duration,assess.coughing_duration) <=3 AND greatest(assess.fever_duration,assess.diarrhea_duration,assess.coughing_duration) <> 0) AS within_72,
	(greatest(assess.fever_duration,assess.diarrhea_duration,assess.coughing_duration) > 3) AS beyond_72,
	
	(assess.referral_follow_up = 'true' OR assess.treatment_follow_up = 'true') AS fu_rec,
	assess.referral_follow_up = 'true' AS fu_ref_rec,
	assess.treatment_follow_up = 'true' AS fu_tx_rec,
		
    (
     CASE
    	WHEN 
    	((assess.diagnosis_fever = 'malaria1' OR assess.diagnosis_fever = 'malaria2') AND assess.patient_age_in_months >=4) --act_rec,
    	--OR (assess.gave_act = 'yes' OR assess.gave_act = 'no')
    	OR (assess.diagnosis_cough = 'pneumonia1' OR assess.diagnosis_cough = 'pneumonia2b' OR assess.diagnosis_cough = 'pneumonia2c') --amox_rec
    	--OR (assess.gave_amox = 'yes' OR assess.gave_amox = 'no')
    	OR (assess.diagnosis_diarrhea <> '' AND assess.patient_age_in_months >=6) --ors_rec
    	--OR (assess.gave_ors = 'yes' OR assess.gave_ors = 'no')
    	OR (assess.diagnosis_diarrhea <> '' AND assess.patient_age_in_months >=2) --zinc_rec
    	--OR (assess.gave_zinc = 'yes' OR assess.gave_zinc = 'no')
    	THEN TRUE
    	ELSE FALSE
		END
    	--OR (assess.diagnosis_fever <> '' AND assess.patient_age_in_months >=3) --paracetamol_rec
    	--OR ((assess.diagnosis_cough = 'cough1' OR assess.diagnosis_cough = 'pneumonia1') AND assess.patient_age_in_months >=24) --cough_syrup_rec
    	
    ) AS fu_tx_needed_during_ax,
    	--CASE
    	--WHEN 
    	--(assess.gave_amox = 'yes' OR assess.gave_ors = 'yes' OR assess.gave_zinc = 'yes' OR assess.gave_act = 'yes') 
    	--THEN TRUE
    	--ELSE FALSE
    	--END 
    FALSE AS fu_tx_given_during_ax /* This is always FALSE for Brac, since they don't capture this info on ax form */
	 
	FROM
		{{ ref("useview_assessment") }} AS assess
	WHERE
		
		/* restrict patient age */
		assess.patient_age_in_months >=2
		AND assess.patient_age_in_months < 12

		/* restrict to only symptomatic assessments */
		AND (assess.patient_coughs = 'yes'
		OR assess.patient_diarrhea = 'yes'
		OR assess.patient_fever = 'yes'
		OR assess.danger_signs <> '') 
	ORDER BY
		assess.uuid