{{
    config(
        materialized = 'incremental',
        indexes=[
            {'columns': ['pregnancy_outcome']},
            {'columns': ['reported']},
            {'columns': ['delivery_date']},
            {'columns': ['reported_by']},
            {'columns': ['reported_by_parent']},
            {'columns': ['patient_id']},
            {'columns': ['reported_by']},
            {'columns': ['health_facility_delivery']},
            {'columns': ['"@timestamp"']}
        ]
    )
}}

	SELECT
	        "@timestamp"::timestamp without time zone AS "@timestamp",
			doc ->> '_id'::text AS uuid,
			doc ->> 'form'::text AS form,
			doc #>> '{contact,_id}'::text[] AS chw,
			doc #>> '{contact,_id}'::text[] AS reported_by,
			doc #>> '{contact,parent,_id}'::text[] AS reported_by_parent,
			COALESCE(doc #>> '{fields,inputs,meta,location,lat}'::text[], ''::text) AS latitude,
			COALESCE(doc #>> '{fields,inputs,meta,location,long}'::text[], ''::text) AS longitude,
			COALESCE(doc #>> '{fields,inputs,meta,location,error}'::text[], ''::text) AS geo_error,
			COALESCE(doc #>> '{fields,inputs,meta,location,message}'::text[], ''::text) AS geo_message,
			doc #>> '{fields,group_vaccine_follow_up,vaccine_follow_up_given}'::text[] AS vaccine_follow_up_given,
			doc #>> '{fields,group_visit_no,pnc_type}' AS pnc_type,
			doc #>> '{fields,group_delivery_summary,mother_condition}' AS mother_condition,
			doc #>> '{fields,group_delivery_summary,mother_dead}' AS mother_dead,
			coalesce(doc #>> '{fields,group_delivery_summary,mother_danger_signs}', doc #>> '{fields,group_vaccine_follow_up,g_mother_danger_signs,mother_danger_signs}') AS mother_danger_signs,
			doc #>> '{fields,group_delivery_summary,sex_of_baby}' AS sex_of_baby,
			doc #>> '{fields,group_weight,visited_facility}' AS visited_facility,
			doc #>> '{fields,group_weight,delivery_mode}' AS delivery_mode,
			doc #>> '{fields,group_breastfeeding,breastfed_1hr}' AS breastfed_1hr,
			doc #>> '{fields,group_breastfeeding,other_food}' AS other_food,
			doc #>> '{fields,group_breastfeeding,breastfeeding_ongoing_newborn}' AS breastfeeding_ongoing_newborn,
			doc #>> '{fields,group_vaccine_first_visit,vaccine_status}' AS vaccine_status,
			doc #>> '{fields,group_vaccine_follow_up,pnc_imm_status}' AS pnc_imm_status,
			doc #>> '{fields,group_vaccine_follow_up,imm_pnc_2,select_vaccine_pnc_2}' AS select_vaccine_pnc_2,
			doc #>> '{fields,group_vaccine_follow_up,imm_pnc_3,select_vaccine_pnc_3}' AS select_vaccine_pnc_3,
			doc #>> '{fields,group_vaccine_follow_up,nutrition,breastfeeding_ongoing}' AS breastfeeding_ongoing,
			doc #>> '{fields,group_vaccine_follow_up,mother_fp,fp}' AS fp,
			doc #>> '{fields,group_vaccine_follow_up,mother_fp,fp_used}' AS fp_used,
			doc #>> '{fields,group_vaccine_follow_up,mother_fp,fp_enroll}' AS fp_enroll,
			doc #>> '{fields,group_vaccine_follow_up,mother_fp,fp_choice}' AS fp_choice,
			doc #>> '{fields,group_vaccine_first_visit,vaccine_first_visit}'::text[] AS vaccine_first_visit,
			doc #>> '{fields,group_vaccine_first_visit,first_vaccination_date}'::text[] AS first_vaccination_date,
			doc #>> '{fields,group_repeat,vaccine_repeat,vaccine_follow_up}'::text[] AS vaccines_given,
			doc #>> '{fields,patient_id}'::text[] AS patient_id,
			doc #>> '{fields,follow_up_count}'::text[] AS follow_up_count,
			doc #>> '{fields,pregnancy_outcome}'::text[] AS pregnancy_outcome,
			NULLIF(doc #>> '{fields,delivery_date}'::text[], ''::text) AS delivery_date,
			doc #>> '{fields,baby_weight}'::text[] AS baby_weight,
			COALESCE(doc #>> '{fields,group_weight,health_facility_delivery}'::text[], ''::text) AS health_facility_delivery,
			doc #>> '{fields,group_baby_status_follow_up,newborn_followup_type}'::text[] AS follow_up_type,
			doc #>> '{fields,baby_danger_signs}'::text[] AS baby_danger_signs,
			doc #>> '{fields,follow_up_method}'::text[] AS follow_up_method,
			to_timestamp((NULLIF(doc ->> 'reported_date', '')::bigint / 1000)::double precision) AS reported

		FROM
			{{ ref("couchdb") }}

		WHERE
			doc->>'type' = 'data_record' AND
			doc ->> 'form' = 'postnatal_care'::text
			{% if is_incremental() %}
				AND "@timestamp" > {{ max_existing_timestamp('"@timestamp"') }}
			{% endif %}
