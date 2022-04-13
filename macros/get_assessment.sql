{% macro get_assessment_data(startDate, endDate) %}

    SELECT  
            form.doc ->> '_id'::text AS uuid,
            form.doc #>> '{contact,_id}'::text[] AS chw,
            form.doc #>> '{contact,_id}'::text[] AS reported_by,
            form.doc #>> '{contact,parent,_id}'::text[] AS reported_by_parent,
            form.doc ->> 'form'::text AS form,
            form.doc #>> '{fields,meta,instanceID}'::text[] AS instanceid,
            form.doc #>> '{fields,inputs,contact,sex}'::text[] AS sex,
            to_timestamp((NULLIF(form.doc ->> 'reported_date'::text, ''::text)::bigint / 1000)::double precision) AS reported,
            COALESCE(form.doc #>> '{fields,inputs,meta,location,lat}'::text[], COALESCE((xpath('/assessment/inputs/meta/location/lat/text()'::text, (form.doc ->> 'content'::text)::xml))[1]::text, ''::text)) AS latitude,
            COALESCE(form.doc #>> '{fields,inputs,meta,location,long}'::text[], COALESCE((xpath('/assessment/inputs/meta/location/long/text()'::text, (form.doc ->> 'content'::text)::xml))[1]::text, ''::text)) AS longitude,
            COALESCE(form.doc #>> '{fields,inputs,meta,location,error}'::text[], ''::text) AS geo_error,
            COALESCE(form.doc #>> '{fields,inputs,meta,location,message}'::text[], ''::text) AS geo_message,
            form.doc #>> '{fields,patient_id}'::text[] AS patient_id,
            CASE
                WHEN (form.doc #>> '{fields,patient_age_in_days}'::text[]) = ''::text OR (form.doc #>> '{fields,patient_age_in_days}'::text[]) ~ 'NaN'::text 
                THEN 99
                ELSE (form.doc #>> '{fields,patient_age_in_days}'::text[])::integer
            END AS patient_age_in_days,
            CASE
                WHEN (form.doc #>> '{fields,patient_age_in_months}'::text[]) = ''::text OR (form.doc #>> '{fields,patient_age_in_months}'::text[]) ~ 'NaN'::text 
                THEN 99
                ELSE (form.doc #>> '{fields,patient_age_in_months}'::text[])::integer
            END AS patient_age_in_months,
            CASE
                WHEN (form.doc #>> '{fields,patient_age_in_years}'::text[]) = ''::text OR (form.doc #>> '{fields,patient_age_in_years}'::text[]) ~ 'NaN'::text 
                THEN 99
                ELSE (form.doc #>> '{fields,patient_age_in_years}'::text[])::integer
            END AS patient_age_in_years,
            form.doc #>> '{fields,group_cough,patient_coughs}'::text[] AS patient_coughs,
            CASE
                WHEN (form.doc #>> '{fields,group_cough,coughing_duration}'::text[]) = ''::text 
                THEN 0
                ELSE (form.doc #>> '{fields,group_cough,coughing_duration}'::text[])::integer
            END AS coughing_duration,
            form.doc #>> '{fields,group_cough,chest_indrawing}'::text[] AS chest_indrawing,
            form.doc #>> '{fields,group_breathing,breath_count}'::text[] AS breath_count,
            form.doc #>> '{fields,group_breathing,fast_breathing}'::text[] AS fast_breathing,
            form.doc #>> '{fields,group_diarrhea,patient_diarrhea}'::text[] AS patient_diarrhea,
            CASE
                WHEN (form.doc #>> '{fields,group_diarrhea,diarrhea_duration}'::text[]) = ''::text 
                THEN 0
                ELSE (form.doc #>> '{fields,group_diarrhea,diarrhea_duration}'::text[])::integer
            END AS diarrhea_duration,
            form.doc #>> '{fields,group_diarrhea,diarrhea_blood}'::text[] AS diarrhea_blood,
            form.doc #>> '{fields,group_fever,patient_fever}'::text[] AS patient_fever,
            CASE
                WHEN (form.doc #>> '{fields,group_fever,fever_duration}'::text[]) = ''::text 
                THEN 0
                ELSE (form.doc #>> '{fields,group_fever,fever_duration}'::text[])::integer
            END AS fever_duration,
            form.doc #>> '{fields,group_fever,patient_temperature}'::text[] AS patient_temperature,
            form.doc #>> '{fields,group_fever,mrdt_result}'::text[] AS mrdt_result,
            form.doc #>> '{fields,group_fever,mrdt_source}'::text[] AS mrdt_source,
            form.doc #>> '{fields,group_diagnosis,diagnosis_cough}'::text[] AS diagnosis_cough,
            form.doc #>> '{fields,group_diagnosis,diagnosis_diarrhea}'::text[] AS diagnosis_diarrhea,
            form.doc #>> '{fields,group_diagnosis,diagnosis_fever}'::text[] AS diagnosis_fever,
            form.doc #>> '{fields,group_fever,malaria_treatment}' AS malaria_treatment,
            form.doc #>> '{fields,group_diarrhea,diarrhea_treatment}' AS diarrhea_treatment,
            form.doc #>> '{fields,group_breathing,pneumonia_treatment}' AS pneumonia_treatment,
            form.doc #>> '{fields,group_danger_signs,danger_signs}'::text[] AS danger_signs,
            form.doc #>> '{fields,treatment_follow_up}'::text[] AS treatment_follow_up,
            form.doc #>> '{fields,referral_follow_up}'::text[] AS referral_follow_up,
            form.doc #>> '{fields,group_imm,group_imm_less_2mo,imm_given_2mo}'::text[] AS imm_given_2mo,
            form.doc #>> '{fields,group_imm,group_imm_2mo_9mo,imm_given_9mo}'::text[] AS imm_given_9mo,
            form.doc #>> '{fields,group_imm,group_imm_9mo_18mo,imm_given_18mo}'::text[] AS imm_given_18mo,
            form.doc #>> '{fields,group_deworm_vit, vit_received}'::text[] AS vit_received,
            form.doc #>> '{fields,group_deworm_vit, deworming_received}'::text[] AS deworming_received,
            NULLIF(form.doc #>> '{fields,group_nutrition_assessment, muac_score}'::text[], '')::double precision AS muac_score,
            form.doc #>> '{fields,group_nutrition_assessment, has_oedema}'::text[] AS has_oedema
        FROM {{ ref("couchdb") }} AS form
        WHERE (form.doc ->> 'form'::text) = 'assessment'::text
        AND to_timestamp((NULLIF(form.doc ->> 'reported_date'::text, ''::text)::bigint / 1000)::double precision) BETWEEN {{startDate}} AND {{endDate}}

{% endmacro %}