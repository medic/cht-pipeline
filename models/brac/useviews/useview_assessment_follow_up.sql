{{
    config(
        materialized = 'view',
        unique_key='useview_assessment_follow_up_source_date_uuid',
        indexes=[
            {'columns': ['reported']},
            {'columns': ['form_source_id']},
            {'columns': ['chw']},
            {'columns': ['reported_by']},
            {'columns': ['reported_by_parent']},
            {'columns': ['uuid']},
        ]
    )
}}
 
SELECT
{{ dbt_utils.surrogate_key([form_source_id, reported, uuid]) }} AS useview_assessment_follow_up_source_date_uuid,
*
FROM(

	SELECT
			form.doc->>'_id' as uuid,
			form.doc->> 'form' AS form,
			form.doc #>> '{contact,_id}' AS chw,
			form.doc #>> '{contact,_id}' AS reported_by,
			form.doc #>> '{contact,parent,_id}' AS reported_by_parent,
			to_timestamp((NULLIF(form.doc ->> 'reported_date'::text, ''::text)::bigint / 1000)::double precision) AS reported,
			COALESCE(form.doc #>> '{fields,form_source_id}','') AS form_source_id,
			form.doc #>> '{fields,patient_id}' AS patient_id,
			
			CASE
				WHEN (form.doc #>> '{fields,patient_age_in_years}') IS NULL OR (form.doc #>> '{fields,patient_age_in_years}') = ''
				THEN 99::int
				ELSE (form.doc #>> '{fields,patient_age_in_years}')::int
			END AS patient_age_in_years,
			
			CASE
				WHEN (form.doc #>> '{fields,patient_age_in_months}') IS NULL OR (form.doc #>> '{fields,patient_age_in_months}') = ''
				THEN 99::int
				ELSE (form.doc #>> '{fields,patient_age_in_months}')::int
			END AS patient_age_in_months,
					
			form.doc #>> '{fields,referral_follow_up_needed}' AS referral_follow_up_needed,
			form.doc #>> '{fields,follow_up_count}' AS follow_up_count,
			form.doc #>> '{fields,patient_health_facility_visit}' AS patient_health_facility_visit,
			form.doc #>> '{fields,group_followup_options,follow_up_type}' AS follow_up_type,
			form.doc #>> '{fields,group_followup_options,follow_up_method}' AS follow_up_method,
			form.doc #>> '{fields,danger_signs}' AS danger_signs,
			form.doc #>> '{fields,patient_improved}' AS patient_improved,
			form.doc #>> '{fields,patient_better}' AS patient_better,
			form.doc #>> '{fields,group_improved,g_patient_treatment_outcome}' AS g_patient_treatment_outcome,
			form.doc #>> '{fields,group_better,g_patient_better}' AS g_patient_referral_outcome

		FROM
			{{ ref("couchdb") }} AS form
					
		WHERE
			form.doc ->> 'form' = 'assessment_follow_up'
) x