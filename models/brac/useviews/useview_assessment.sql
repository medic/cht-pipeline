{{
    config(
        materialized = 'incremental',
        unique_key='useview_assessment_reported_age_uuid',
        indexes=[
            {'columns': ['reported']},
            {'columns': ['chw']},
            {'columns': ['rev_id']},
            {'columns': ['reported_by']},
            {'columns': ['reported_by_parent']},
            {'columns': ['referral_follow_up']},
            {'columns': ['uuid']},
            {'columns': ['"@timestamp"']}
        ]
    )
}}


    SELECT
            "@timestamp"::timestamp without time zone AS "@timestamp",
            doc ->> '_id'::text AS uuid,
            doc ->> '_rev'::text AS rev_id,
            doc #>> '{contact,_id}'::text[] AS chw,
            doc #>> '{contact,_id}'::text[] AS reported_by,
            doc #>> '{contact,parent,_id}'::text[] AS reported_by_parent,
            doc ->> 'form'::text AS form,
            doc #>> '{fields,meta,instanceID}'::text[] AS instanceid,
            doc #>> '{fields,inputs,contact,sex}'::text[] AS sex,
            to_timestamp((NULLIF(doc ->> 'reported_date'::text, ''::text)::bigint / 1000)::double precision) AS reported,
            COALESCE(doc #>> '{fields,inputs,meta,location,lat}'::text[], COALESCE((xpath('/assessment/inputs/meta/location/lat/text()'::text, (doc ->> 'content'::text)::xml))[1]::text, ''::text)) AS latitude,
            COALESCE(doc #>> '{fields,inputs,meta,location,long}'::text[], COALESCE((xpath('/assessment/inputs/meta/location/long/text()'::text, (doc ->> 'content'::text)::xml))[1]::text, ''::text)) AS longitude,
            COALESCE(doc #>> '{fields,inputs,meta,location,error}'::text[], ''::text) AS geo_error,
            COALESCE(doc #>> '{fields,inputs,meta,location,message}'::text[], ''::text) AS geo_message,
            doc #>> '{fields,patient_id}'::text[] AS patient_id,
            CASE
                WHEN (doc #>> '{fields,patient_age_in_days}'::text[]) = ''::text OR (doc #>> '{fields,patient_age_in_days}'::text[]) ~ 'NaN'::text
                THEN 99
                ELSE (doc #>> '{fields,patient_age_in_days}'::text[])::integer
            END AS patient_age_in_days,
            CASE
                WHEN (doc #>> '{fields,patient_age_in_months}'::text[]) = ''::text OR (doc #>> '{fields,patient_age_in_months}'::text[]) ~ 'NaN'::text
                THEN 99
                ELSE (doc #>> '{fields,patient_age_in_months}'::text[])::integer
            END AS patient_age_in_months,
            CASE
                WHEN (doc #>> '{fields,patient_age_in_years}'::text[]) = ''::text OR (doc #>> '{fields,patient_age_in_years}'::text[]) ~ 'NaN'::text
                THEN 99
                ELSE (doc #>> '{fields,patient_age_in_years}'::text[])::integer
            END AS patient_age_in_years,
            doc #>> '{fields,group_cough,patient_coughs}'::text[] AS patient_coughs,
            CASE
                WHEN (doc #>> '{fields,group_cough,coughing_duration}'::text[]) = ''::text
                THEN 0
                ELSE (doc #>> '{fields,group_cough,coughing_duration}'::text[])::integer
            END AS coughing_duration,
            doc #>> '{fields,group_cough,chest_indrawing}'::text[] AS chest_indrawing,
            doc #>> '{fields,group_breathing,breath_count}'::text[] AS breath_count,
            doc #>> '{fields,group_breathing,fast_breathing}'::text[] AS fast_breathing,
            doc #>> '{fields,group_diarrhea,patient_diarrhea}'::text[] AS patient_diarrhea,
            CASE
                WHEN (doc #>> '{fields,group_diarrhea,diarrhea_duration}'::text[]) = ''::text
                THEN 0
                ELSE (doc #>> '{fields,group_diarrhea,diarrhea_duration}'::text[])::integer
            END AS diarrhea_duration,
            doc #>> '{fields,group_diarrhea,diarrhea_blood}'::text[] AS diarrhea_blood,
            doc #>> '{fields,group_fever,patient_fever}'::text[] AS patient_fever,
            CASE
                WHEN (doc #>> '{fields,group_fever,fever_duration}'::text[]) = ''::text
                THEN 0
                ELSE (doc #>> '{fields,group_fever,fever_duration}'::text[])::integer
            END AS fever_duration,
            doc #>> '{fields,group_fever,patient_temperature}'::text[] AS patient_temperature,
            doc #>> '{fields,group_fever,mrdt_result}'::text[] AS mrdt_result,
            doc #>> '{fields,group_fever,mrdt_source}'::text[] AS mrdt_source,
            doc #>> '{fields,group_diagnosis,diagnosis_cough}'::text[] AS diagnosis_cough,
            doc #>> '{fields,group_diagnosis,diagnosis_diarrhea}'::text[] AS diagnosis_diarrhea,
            doc #>> '{fields,group_diagnosis,diagnosis_fever}'::text[] AS diagnosis_fever,
            doc #>> '{fields,group_fever,malaria_treatment}' AS malaria_treatment,
            doc #>> '{fields,group_diarrhea,diarrhea_treatment}' AS diarrhea_treatment,
            doc #>> '{fields,group_breathing,pneumonia_treatment}' AS pneumonia_treatment,
            doc #>> '{fields,group_danger_signs,danger_signs}'::text[] AS danger_signs,
            doc #>> '{fields,treatment_follow_up}'::text[] AS treatment_follow_up,
            doc #>> '{fields,referral_follow_up}'::text[] AS referral_follow_up,
            doc #>> '{fields,group_imm,group_imm_less_2mo,imm_given_2mo}'::text[] AS imm_given_2mo,
            doc #>> '{fields,group_imm,group_imm_2mo_9mo,imm_given_9mo}'::text[] AS imm_given_9mo,
            doc #>> '{fields,group_imm,group_imm_9mo_18mo,imm_given_18mo}'::text[] AS imm_given_18mo,
            doc #>> '{fields,group_deworm_vit, vit_received}'::text[] AS vit_received,
            doc #>> '{fields,group_deworm_vit, deworming_received}'::text[] AS deworming_received,
            NULLIF(doc #>> '{fields,group_nutrition_assessment, muac_score}'::text[], '')::double precision AS muac_score,
            doc #>> '{fields,group_nutrition_assessment, has_oedema}'::text[] AS has_oedema
        FROM {{ ref("couchdb") }} AS form
        WHERE  doc->>'type' = 'data_record' AND (doc ->> 'form'::text) = 'assessment'::text
        {% if is_incremental() %}
            AND "@timestamp" > {{ max_existing_timestamp('"@timestamp"') }}
        {% endif %}