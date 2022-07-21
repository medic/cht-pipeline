WITH follow_up_activity_cte AS (
         SELECT date_trunc('month'::text, assess_fu.reported) AS reported_month,
            chw_facility.facility_join_field,
            count(DISTINCT assess_fu.form_source_id) AS follow_up_count
           FROM ({{ ref("useview_assessment_follow_up") }} assess_fu
             JOIN {{ ref("impactview_chw_facility") }} chw_facility ON ((assess_fu.chw = chw_facility.chw_uuid)))
          WHERE (assess_fu.patient_age_in_years < 5)
          GROUP BY date_trunc('month'::text, assess_fu.reported), chw_facility.facility_join_field
        ), follow_up_deduped_cte AS (
         SELECT DISTINCT ON (useview_assessment_follow_up.form_source_id) useview_assessment_follow_up.form_source_id,
            useview_assessment_follow_up.reported,
            useview_assessment_follow_up.patient_health_facility_visit
           FROM {{ ref("useview_assessment_follow_up") }}
          WHERE (((useview_assessment_follow_up.follow_up_count = '1'::text) AND (useview_assessment_follow_up.form_source_id <> ''::text)) AND (useview_assessment_follow_up.form_source_id IS NOT NULL))
          ORDER BY useview_assessment_follow_up.form_source_id, useview_assessment_follow_up.reported
        ), assessment_and_fu_cte AS (
         SELECT (date_trunc('month'::text, assess.reported))::date AS reported_month,
            chw_facility.facility_join_field,
            count(assess.*) AS assessment_count,
            sum(
                CASE
                    WHEN (assess.diagnosis_fever ~~ 'malaria%'::text) THEN 1
                    ELSE 0
                END) AS malaria_diagnoses,
            sum(
                CASE
                    WHEN (assess.diagnosis_diarrhea ~~ 'diarrhea%'::text) THEN 1
                    ELSE 0
                END) AS diarrhea_diagnoses,
            sum(
                CASE
                    WHEN (assess.diagnosis_cough ~~ 'pneumonia%'::text) THEN 1
                    ELSE 0
                END) AS pneumonia_diagnoses,
            sum(
                CASE
                    WHEN ((((assess.patient_fever <> 'yes'::text) AND (assess.patient_diarrhea <> 'yes'::text)) AND (assess.patient_coughs <> 'yes'::text)) AND (assess.danger_signs = ''::text)) THEN 1
                    ELSE 0
                END) AS no_symptoms,
            sum(
                CASE
                    WHEN (assess.treatment_follow_up = 'true'::text) THEN 1
                    ELSE 0
                END) AS treatment_fu_rec,
            sum(
                CASE
                    WHEN (assess.referral_follow_up = 'true'::text) THEN 1
                    ELSE 0
                END) AS referral_fu_rec,
            sum(
                CASE
                    WHEN ((assess.referral_follow_up = 'true'::text) OR (assess.treatment_follow_up = 'true'::text)) THEN 1
                    ELSE 0
                END) AS treat_or_refer_fu_rec,
            sum(
                CASE
                    WHEN ((assess.referral_follow_up = 'true'::text) AND (assess.treatment_follow_up = 'true'::text)) THEN 1
                    ELSE 0
                END) AS treat_and_refer_fu_rec,
            sum(
                CASE
                    WHEN ((assess.treatment_follow_up = 'true'::text) AND (follow_up.reported IS NOT NULL)) THEN 1
                    ELSE 0
                END) AS treatment_fu_confirmed,
            sum(
                CASE
                    WHEN ((assess.treatment_follow_up = 'true'::text) AND (date_part('days'::text, (follow_up.reported - assess.reported)) < (3)::double precision)) THEN 1
                    ELSE 0
                END) AS treatment_fu_confirmed_on_time,
            sum(
                CASE
                    WHEN ((assess.referral_follow_up = 'true'::text) AND (follow_up.reported IS NOT NULL)) THEN 1
                    ELSE 0
                END) AS referral_fu_confirmed,
            sum(
                CASE
                    WHEN ((assess.referral_follow_up = 'true'::text) AND (date_part('days'::text, (follow_up.reported - assess.reported)) < (3)::double precision)) THEN 1
                    ELSE 0
                END) AS referral_fu_confirmed_on_time,
            sum(
                CASE
                    WHEN ((assess.referral_follow_up = 'true'::text) AND (follow_up.patient_health_facility_visit = 'yes'::text)) THEN 1
                    ELSE 0
                END) AS referral_fu_confirmed_health_facility
           FROM (({{ ref("useview_assessment") }} assess
             JOIN {{ ref("impactview_chw_facility") }} chw_facility ON ((assess.chw = chw_facility.chw_uuid)))
             LEFT JOIN follow_up_deduped_cte follow_up ON ((assess.uuid = follow_up.form_source_id)))
          WHERE (assess.patient_age_in_years < 5)
          GROUP BY (date_trunc('month'::text, assess.reported))::date, chw_facility.facility_join_field
        ), impactview_month_facility_cte AS (
            SELECT 
                generate_series(date_trunc('month', now() - '1 year'::interval), now(), '1 mon'::interval)::date AS month,
                date_part('epoch', generate_series(date_trunc('month', now() - '1 year'::interval), now(), '1 mon'::interval)::date) AS epoch
            ORDER BY
                epoch
        )
 SELECT month_facility.month,
    month_facility.epoch,
    month_facility.facility_name,
    month_facility.facility_join_field,
    COALESCE(assessment_and_fu_cte.assessment_count, (0)::bigint) AS assessment_count,
    COALESCE(follow_up_activity_cte.follow_up_count, (0)::bigint) AS follow_up_count,
    COALESCE(assessment_and_fu_cte.malaria_diagnoses, (0)::bigint) AS malaria_diagnoses,
    COALESCE(assessment_and_fu_cte.diarrhea_diagnoses, (0)::bigint) AS diarrhea_diagnoses,
    COALESCE(assessment_and_fu_cte.pneumonia_diagnoses, (0)::bigint) AS pneumonia_diagnoses,
    COALESCE(assessment_and_fu_cte.no_symptoms, (0)::bigint) AS no_symptoms,
    COALESCE(assessment_and_fu_cte.treatment_fu_rec, (0)::bigint) AS treatment_fu_rec,
    COALESCE(assessment_and_fu_cte.referral_fu_rec, (0)::bigint) AS referral_fu_rec,
    COALESCE(assessment_and_fu_cte.treat_or_refer_fu_rec, (0)::bigint) AS treat_or_refer_fu_rec,
    COALESCE(assessment_and_fu_cte.treat_and_refer_fu_rec, (0)::bigint) AS treat_and_refer_fu_rec,
    COALESCE(assessment_and_fu_cte.treatment_fu_confirmed, (0)::bigint) AS treatment_fu_confirmed,
    COALESCE(assessment_and_fu_cte.treatment_fu_confirmed_on_time, (0)::bigint) AS treatment_fu_confirmed_on_time,
    COALESCE(assessment_and_fu_cte.referral_fu_confirmed, (0)::bigint) AS referral_fu_confirmed,
    COALESCE(assessment_and_fu_cte.referral_fu_confirmed_on_time, (0)::bigint) AS referral_fu_confirmed_on_time,
    COALESCE(assessment_and_fu_cte.referral_fu_confirmed_health_facility, (0)::bigint) AS referral_fu_confirmed_health_facility
   FROM ((impactview_month_facility_cte month_facility
     LEFT JOIN assessment_and_fu_cte ON (((month_facility.facility_join_field = assessment_and_fu_cte.facility_join_field) AND (month_facility.month = assessment_and_fu_cte.reported_month))))
     LEFT JOIN follow_up_activity_cte ON (((month_facility.facility_join_field = follow_up_activity_cte.facility_join_field) AND (month_facility.month = follow_up_activity_cte.reported_month))))
  ORDER BY month_facility.month, month_facility.facility_name