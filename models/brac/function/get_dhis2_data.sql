{{ config(materialized = 'raw_sql') }}  


CREATE OR REPLACE FUNCTION {{ this }}(starting_date timestamp, ending_date timestamp)
RETURNS TABLE(
  facility_name text,
  reporting_period text,
  interval_start date,
  interval_end date,
  num_hh integer,
  chps_reporting_iccm integer,
  chps_reporting_fp integer,
  chps_submitted_report integer,
  chps_expected integer,
  num_villages_reporting integer,
  tot_u1_month_female integer,
  tot_u1_month_male integer,
  u1_month_deaths_female integer,
  u1_month_deaths_male integer,
  tot_1_11_months_female integer,
  tot_1_11_months_male integer,
  _1_11_month_deaths_female integer,
  _1_11_month_deaths_male integer,
  tot_1_5_years_female integer,
  tot_1_5_years_male integer,
  _1_5_years_deaths_female integer,
  _1_5_years_deaths_male integer,
  tot_u5_female integer,
  tot_u5_male integer,
  vit_a_female integer,
  vit_a_male integer,
  deworming_received_female integer,
  deworming_received_male integer,
  muac_screened_female integer,
  muac_screened_male integer,
  yellow_muac_female integer,
  yellow_muac_male integer,
  red_muac_female integer,
  red_muac_male integer,
  has_oedema_female integer,
  has_oedema_male integer,
  reffered_muac_female integer,
  reffered_muac_male integer,
  linked_to_care_female integer,
  linked_to_care_male integer,
  imm_upto_date_female integer,
  imm_upto_date_male integer,
  has_disability_female integer,
  has_disability_male integer,
  fp_info_and_methods_female integer,
  fp_info_and_methods_male integer,
  home_deliveries integer,
  atleast_4_anc integer,
  atleast_8_anc integer,
  tot_adoloscents_female integer,
  tot_adoloscents_male integer,
  latrine integer,
  improved_latrine integer,
  handwashing_facility integer,
  source_of_drinking_water integer,
  open_defecation_free integer,
  sick_attended_female integer,
  sick_attended_male integer,
  diarrhea_dx_female integer,
  diarrhea_dx_male integer,
  ors_zinc_pack_female integer,
  ors_zinc_pack_male integer,
  diarrhea_treat_within_24_female integer,
  diarrhea_treat_within_24_male integer,
  fever_female integer,
  fever_male integer,
  mrdt_female integer,
  mrdt_male integer,
  mrdt_positive_female integer,
  mrdt_positive_male integer,
  received_act_female integer,
  received_act_male integer,
  fever_danger_signs_female integer,
  fever_danger_signs_male integer,
  fever_treat_within_24_female integer,
  fever_treat_within_24_male integer,
  pneumonia_dx_female integer,
  pneumonia_dx_male integer,
  received_amoxicillin_female integer,
  received_amoxicillin_male integer,
  pneumonia_treat_within_24_female integer,
  pneumonia_treat_within_24_male integer,
  recovered_female integer,
  recovered_male integer,
  sick_referred_female integer,
  sick_referred_male integer
)
 LANGUAGE sql
 STABLE
AS $function$

SELECT 
  facility_name,
  reporting_period,
  date(starting_date) AS interval_start,
  date(ending_date) AS interval_end,
  SUM(COALESCE(FAMILYREG.COUNT, 0))::int AS num_hh,
  SUM(COALESCE(ASSESS.COUNT, 0))::int AS chps_reporting_iccm,
  SUM(COALESCE(FP_RECORDS.COUNT, 0))::int AS chps_reporting_fp,
  SUM(COALESCE(REPORTING.COUNT, 0))::int AS chps_submitted_report,
  COUNT(uuid)::int AS chps_expected,

  ARRAY_LENGTH(
    ARRAY(SELECT DISTINCT unnest(STRING_TO_ARRAY(STRING_AGG(villages_served, ','), ','))), 1
  ) AS num_villages_reporting,
  
  SUM(COALESCE(tot_u1_month_female, 0))::int AS tot_u1_month_female,
  SUM(COALESCE(tot_u1_month_male, 0))::int AS tot_u1_month_male,
  SUM(COALESCE(u1_month_deaths_female, 0))::int AS u1_month_deaths_female,
  SUM(COALESCE(u1_month_deaths_male, 0))::int AS u1_month_deaths_male,
  SUM(COALESCE(tot_1_11_months_female, 0))::int AS tot_1_11_months_female,
  SUM(COALESCE(tot_1_11_months_male, 0))::int AS tot_1_11_months_male,
  SUM(COALESCE(_1_11_month_deaths_female, 0))::int AS _1_11_month_deaths_female,
  SUM(COALESCE(_1_11_month_deaths_male, 0))::int AS _1_11_month_deaths_male,
  SUM(COALESCE(tot_1_5_years_female, 0))::int AS tot_1_5_years_female,
  SUM(COALESCE(tot_1_5_years_male, 0))::int AS tot_1_5_years_male,
  SUM(COALESCE(_1_5_years_deaths_female, 0))::int AS _1_5_years_deaths_female,
  SUM(COALESCE(_1_5_years_deaths_male, 0))::int AS _1_5_years_deaths_male,
  SUM(COALESCE(tot_u5_female, 0))::int AS tot_u5_female,
  SUM(COALESCE(tot_u5_male, 0))::int AS tot_u5_male,
  SUM(COALESCE(vit_a_female, 0))::int AS vit_a_female,
  SUM(COALESCE(vit_a_male, 0))::int AS vit_a_male,
  SUM(COALESCE(deworming_received_female, 0))::int AS deworming_received_female,
  SUM(COALESCE(deworming_received_male, 0))::int AS deworming_received_male,
  SUM(COALESCE(muac_screened_female, 0))::int AS muac_screened_female,
  SUM(COALESCE(muac_screened_male, 0))::int AS muac_screened_male,
  SUM(COALESCE(yellow_muac_female, 0))::int AS yellow_muac_female,
  SUM(COALESCE(yellow_muac_male, 0))::int AS yellow_muac_male,
  SUM(COALESCE(red_muac_female, 0))::int AS red_muac_female,
  SUM(COALESCE(red_muac_male, 0))::int AS red_muac_male,
  SUM(COALESCE(has_oedema_female, 0))::int AS has_oedema_female,
  SUM(COALESCE(has_oedema_male, 0))::int AS has_oedema_male,

  (SUM(COALESCE(yellow_muac_female, 0))::int + SUM(COALESCE(red_muac_female, 0))::int) AS reffered_muac_female,
  (SUM(COALESCE(yellow_muac_male, 0))::int + SUM(COALESCE(red_muac_male, 0))::int) AS reffered_muac_male,
  
  SUM(COALESCE(linked_to_care_female, 0))::int AS linked_to_care_female,
  SUM(COALESCE(linked_to_care_male, 0))::int AS linked_to_care_male,
  SUM(COALESCE(imm_upto_date_female, 0))::int AS imm_upto_date_female,
  SUM(COALESCE(imm_upto_date_male, 0))::int AS imm_upto_date_male,
  SUM(COALESCE(has_disability_female, 0))::int AS has_disability_female,
  SUM(COALESCE(has_disability_male, 0))::int AS has_disability_male,
  SUM(COALESCE(fp_info_and_methods_female, 0))::int AS fp_info_and_methods_female,
  SUM(COALESCE(fp_info_and_methods_male, 0))::int AS fp_info_and_methods_male,
  SUM(COALESCE(home_deliveries, 0))::int AS home_deliveries,
  SUM(COALESCE(atleast_4_anc, 0))::int AS atleast_4_anc,
  SUM(COALESCE(atleast_8_anc, 0))::int AS atleast_8_anc,
  SUM(COALESCE(tot_adoloscents_female, 0))::int AS tot_adoloscents_female,
  SUM(COALESCE(tot_adoloscents_male, 0))::int AS tot_adoloscents_male,
  SUM(COALESCE(latrine, 0))::int AS latrine,
  SUM(COALESCE(g_improved_latrine, 0))::int AS improved_latrine,
  SUM(COALESCE(g_handwashing_facility, 0))::int AS handwashing_facility,
  SUM(COALESCE(source_of_drinking_water, 0))::int AS source_of_drinking_water,
  SUM(COALESCE(g_open_defecation_free, 0))::int AS open_defecation_free,
  SUM(COALESCE(sick_attended_female, 0))::int AS sick_attended_female,
  SUM(COALESCE(sick_attended_male, 0))::int AS sick_attended_male,
  SUM(COALESCE(diarrhea_dx_female, 0))::int AS diarrhea_dx_female,
  SUM(COALESCE(diarrhea_dx_male, 0))::int AS diarrhea_dx_male,
  SUM(COALESCE(ors_zinc_pack_female, 0))::int AS ors_zinc_pack_female,
  SUM(COALESCE(ors_zinc_pack_male, 0))::int AS ors_zinc_pack_male,
  SUM(COALESCE(diarrhea_treat_within_24_female, 0))::int AS diarrhea_treat_within_24_female,
  SUM(COALESCE(diarrhea_treat_within_24_male, 0))::int AS diarrhea_treat_within_24_male,
  SUM(COALESCE(fever_female, 0))::int AS fever_female,
  SUM(COALESCE(fever_male, 0))::int AS fever_male,
  SUM(COALESCE(mrdt_female, 0))::int AS mrdt_female,
  SUM(COALESCE(mrdt_male, 0))::int AS mrdt_male,
  SUM(COALESCE(mrdt_positive_female, 0))::int AS mrdt_positive_female,
  SUM(COALESCE(mrdt_positive_male, 0))::int AS mrdt_positive_male,
  SUM(COALESCE(received_act_female, 0))::int AS received_act_female,
  SUM(COALESCE(received_act_male, 0))::int AS received_act_male,
  SUM(COALESCE(fever_danger_signs_female, 0))::int AS fever_danger_signs_female,
  SUM(COALESCE(fever_danger_signs_male, 0))::int AS fever_danger_signs_male,
  SUM(COALESCE(fever_treat_within_24_female, 0))::int AS fever_treat_within_24_female,
  SUM(COALESCE(fever_treat_within_24_male, 0))::int AS fever_treat_within_24_male,
  SUM(COALESCE(pneumonia_dx_female, 0))::int AS pneumonia_dx_female,
  SUM(COALESCE(pneumonia_dx_male, 0))::int AS pneumonia_dx_male,
  SUM(COALESCE(received_amox_female, 0))::int AS received_amoxicillin_female,
  SUM(COALESCE(received_amox_male, 0))::int AS received_amoxicillin_male,
  SUM(COALESCE(pneumonia_treat_within_24_female, 0))::int AS pneumonia_treat_within_24_female,
  SUM(COALESCE(pneumonia_treat_within_24_male, 0))::int AS pneumonia_treat_within_24_male,
  SUM(COALESCE(recovered_female, 0))::int AS recovered_female,
  SUM(COALESCE(recovered_male, 0))::int AS recovered_male,
  SUM(COALESCE(referred_female, 0))::int AS sick_referred_female,
  SUM(COALESCE(referred_male, 0))::int AS sick_referred_male

FROM 
  {{ ref("contactview_chp") }} chp
LEFT JOIN
(
  WITH month_series AS (
    SELECT 
      1 as id,
      to_char(generate_series(date_trunc('day',starting_date), ending_date, '1 month'::interval), 'Month') months
    )
  SELECT string_agg(trim(months), ', ') AS reporting_period
  FROM month_series
  GROUP BY id
) REPORT_PERIOD ON TRUE

LEFT JOIN
(
  SELECT
		family.parent_uuid AS area_uuid,
    COUNT(person.uuid) FILTER(
			WHERE 
       (date_part('year', age(now(), to_date(person.date_of_birth,'YYYY-MM-DD'))) * 12 + 
       date_part('month', age(now(), to_date(person.date_of_birth,'YYYY-MM-DD')))) < 1
			 AND person.sex ='female' AND (date_trunc('hour',person.reported)::DATE) <= date_trunc('hour',ending_date)::DATE
		) AS tot_u1_month_female,
    COUNT(person.uuid) FILTER(
			WHERE 
       (date_part('year', age(now(), to_date(person.date_of_birth,'YYYY-MM-DD'))) * 12 + 
       date_part('month', age(now(), to_date(person.date_of_birth,'YYYY-MM-DD')))) < 1
			 AND person.sex ='male' AND (date_trunc('hour',person.reported)::DATE) <= date_trunc('hour',ending_date)::DATE
		) AS tot_u1_month_male,

    COUNT(person.uuid) FILTER(
			WHERE 
       (date_part('year', age(now(), to_date(person.date_of_birth,'YYYY-MM-DD'))) * 12 + 
       date_part('month', age(now(), to_date(person.date_of_birth,'YYYY-MM-DD')))) BETWEEN 1 AND 11
			 AND person.sex ='female' AND (date_trunc('hour',person.reported)::DATE) <= date_trunc('hour',ending_date)::DATE
		) AS tot_1_11_months_female,
    COUNT(person.uuid) FILTER(
			WHERE 
       (date_part('year', age(now(), to_date(person.date_of_birth,'YYYY-MM-DD'))) * 12 + 
       date_part('month', age(now(), to_date(person.date_of_birth,'YYYY-MM-DD')))) BETWEEN 1 AND 11
			 AND person.sex ='male' AND (date_trunc('hour',person.reported)::DATE) <= date_trunc('hour',ending_date)::DATE
		) AS tot_1_11_months_male,

    COUNT(person.uuid) FILTER(
			WHERE extract (YEAR from age(now()::date, to_date(person.date_of_birth,'YYYY-MM-DD')))::int BETWEEN 1 AND 5  
			AND person.sex ='female' AND (date_trunc('hour',person.reported)::DATE) <= date_trunc('hour',ending_date)::DATE
		) AS tot_1_5_years_female,
    COUNT(person.uuid) FILTER(
			WHERE extract (YEAR from age(now()::date, to_date(person.date_of_birth,'YYYY-MM-DD')))::int BETWEEN 1 AND 5  
			AND person.sex ='male' AND (date_trunc('hour',person.reported)::DATE) <= date_trunc('hour',ending_date)::DATE
		) AS tot_1_5_years_male,

    COUNT(person.uuid) FILTER(
			WHERE extract (YEAR from age(now()::date, to_date(person.date_of_birth,'YYYY-MM-DD')))::int < 5
			AND person.sex ='female' AND (date_trunc('hour',person.reported)::DATE) <= date_trunc('hour',ending_date)::DATE
		) AS tot_u5_female,
    COUNT(person.uuid) FILTER(
			WHERE extract (YEAR from age(now()::date, to_date(person.date_of_birth,'YYYY-MM-DD')))::int < 5
			AND person.sex ='male' AND (date_trunc('hour',person.reported)::DATE) <= date_trunc('hour',ending_date)::DATE
		) AS tot_u5_male,

    COUNT(person.uuid) FILTER(
			WHERE extract (YEAR from age(now()::date, to_date(person.date_of_birth,'YYYY-MM-DD')))::int BETWEEN 10 AND 24
			AND person.sex ='female' AND (date_trunc('hour',person.reported)::DATE) <= date_trunc('hour',ending_date)::DATE
		) AS tot_adoloscents_female,
    COUNT(person.uuid) FILTER(
			WHERE extract (YEAR from age(now()::date, to_date(person.date_of_birth,'YYYY-MM-DD')))::int BETWEEN 10 AND 24
			AND person.sex ='male' AND (date_trunc('hour',person.reported)::DATE) <= date_trunc('hour',ending_date)::DATE
		) AS tot_adoloscents_male,

    COUNT(person.uuid) FILTER(WHERE has_disability = 'yes' AND person.sex = 'female') AS has_disability_female,
    COUNT(person.uuid) FILTER(WHERE has_disability = 'yes' AND person.sex = 'male') AS has_disability_male
	FROM 
		{{ ref("contactview_person") }} person
	LEFT JOIN {{ ref("contactview_metadata") }} AS family ON person.parent_uuid = family.uuid
	WHERE
		family.type = 'clinic'
        AND NOT EXISTS (SELECT NULL FROM {{ ref("get_muted_contacts") }}(ending_date,'person') muted
          WHERE muted.contact_uuid = person.uuid)
	GROUP BY 
		family.parent_uuid
) demography ON chp.area_uuid = demography.area_uuid

LEFT JOIN
(
  SELECT
    parent_uuid AS area_uuid,
    count(*)
  FROM
    {{ ref("contactview_metadata") }} family

  WHERE
    type = 'clinic'
    AND NOT EXISTS (SELECT NULL FROM {{ ref("get_muted_contacts") }}(now(),'clinic') muted
      WHERE muted.contact_uuid = family.uuid)

  GROUP BY
    AREA_UUID
) FAMILYREG ON chp.area_uuid = FAMILYREG.area_uuid

LEFT JOIN
(
  SELECT
    reported_by_parent AS area_uuid,
    COUNT(DISTINCT reported_by)
  FROM
    {{ ref("form_metadata") }}
  WHERE
	reported >= (date_trunc('day',starting_date))::date AND
    reported < (date_trunc('day',ending_date) + '1 day'::interval)::date
  GROUP BY 
	area_uuid
) REPORTING ON chp.area_uuid = REPORTING.area_uuid

LEFT JOIN
(
  SELECT
	reported_by_parent AS  area_uuid,
	COUNT(DISTINCT patient_id) AS COUNT,
	COUNT(DISTINCT patient_id) FILTER(WHERE age_in_months < 1 AND sex = 'female') AS u1_month_deaths_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE age_in_months < 1 AND sex = 'male') AS u1_month_deaths_male,
    COUNT(DISTINCT patient_id) FILTER(WHERE age_in_months BETWEEN 1 AND 11 AND sex = 'female') AS _1_11_month_deaths_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE age_in_months BETWEEN 1 AND 11 AND sex = 'male') AS _1_11_month_deaths_male,
    COUNT(DISTINCT patient_id) FILTER(WHERE age_in_months BETWEEN 12 AND 60 AND sex = 'female') AS _1_5_years_deaths_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE age_in_months BETWEEN 12 AND 60 AND sex = 'male') AS _1_5_years_deaths_male
  FROM {{ ref("formview_death_confirmation") }}
  WHERE
    reported >= (date_trunc('day',starting_date))::date AND
    reported < (date_trunc('day',ending_date) + '1 day'::interval)::date
  GROUP BY 
    area_uuid
) deaths ON deaths.area_uuid = chp.area_uuid

LEFT JOIN
(
  SELECT
	  reported_by_parent  AS  area_uuid,
    COUNT(DISTINCT patient_id) FILTER(WHERE NULLIF(NULLIF(vit_received, 'none'), '') IS NOT NULL AND sex = 'female') AS vit_a_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE NULLIF(NULLIF(vit_received, 'none'), '') IS NOT NULL AND sex = 'male') AS vit_a_male,
    COUNT(DISTINCT patient_id) FILTER(WHERE NULLIF(NULLIF(deworming_received, 'none'), '') IS NOT NULL AND sex = 'female') AS deworming_received_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE NULLIF(NULLIF(deworming_received, 'none'), '') IS NOT NULL AND sex = 'male') AS deworming_received_male
  FROM 
    {{ ref("useview_assessment") }}
  WHERE
    patient_age_in_years < 5 AND
    reported >= date_trunc('day', ending_date - '6 months'::interval)::date AND
    reported < (date_trunc('day',ending_date) + '1 day'::interval)::date
  GROUP BY 
		area_uuid
) DEWORM_VIT_6MTHS ON DEWORM_VIT_6MTHS.area_uuid = chp.area_uuid

LEFT JOIN
(
  SELECT
    reported_by_parent  AS  area_uuid,
    COUNT(DISTINCT reported_by),
    COUNT(DISTINCT patient_id) FILTER(WHERE muac_score IS NOT NULL AND sex = 'female') AS muac_screened_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE muac_score IS NOT NULL AND sex = 'male') AS muac_screened_male,
    COUNT(DISTINCT patient_id) FILTER(WHERE muac_score BETWEEN 11.5 AND 12.5 AND sex = 'female') AS yellow_muac_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE muac_score BETWEEN 11.5 AND 12.5 AND sex = 'male') AS yellow_muac_male,
    COUNT(DISTINCT patient_id) FILTER(WHERE muac_score BETWEEN 1.0 AND 11.4 AND sex = 'female') AS red_muac_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE muac_score BETWEEN 1.0 AND 11.4 AND sex = 'male') AS red_muac_male,
    COUNT(DISTINCT patient_id) FILTER(WHERE has_oedema = 'yes' AND sex = 'female') has_oedema_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE has_oedema = 'yes' AND sex = 'male') has_oedema_male,
    COUNT(DISTINCT patient_id) FILTER(WHERE COALESCE(imm_given_2mo, imm_given_9mo, imm_given_18mo) IS NOT NULL AND sex = 'female') AS imm_upto_date_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE COALESCE(imm_given_2mo, imm_given_9mo, imm_given_18mo) IS NOT NULL AND sex = 'male') AS imm_upto_date_male,
    COUNT(DISTINCT patient_id) FILTER(WHERE sex = 'female' AND 
      (diagnosis_fever ~~ 'malaria%' OR diagnosis_diarrhea ~~ 'diarrhea%' OR diagnosis_cough ~~ 'pneumonia%')) AS sick_attended_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE sex = 'male' AND 
      (diagnosis_fever ~~ 'malaria%' OR diagnosis_diarrhea ~~ 'diarrhea%' OR diagnosis_cough ~~ 'pneumonia%')) AS sick_attended_male,
    COUNT(DISTINCT patient_id) FILTER(WHERE diagnosis_diarrhea ~~ 'diarrhea%' and sex = 'female') AS diarrhea_dx_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE diagnosis_diarrhea ~~ 'diarrhea%' and sex = 'male') AS diarrhea_dx_male,
    COUNT(DISTINCT patient_id) FILTER(WHERE diarrhea_treatment = 'ors zinc' AND sex = 'female') AS ors_zinc_pack_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE diarrhea_treatment = 'ors zinc' AND sex = 'male') AS ors_zinc_pack_male,
    COUNT(DISTINCT patient_id) FILTER(WHERE diarrhea_duration = 1 
      AND NULLIF(diarrhea_treatment, '') IS NOT NULL AND sex = 'female') AS diarrhea_treat_within_24_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE diarrhea_duration = 1 
      AND NULLIF(diarrhea_treatment, '') IS NOT NULL AND sex = 'male') AS diarrhea_treat_within_24_male,
    COUNT(DISTINCT patient_id) FILTER(WHERE patient_fever = 'yes' AND sex = 'female') AS fever_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE patient_fever = 'yes' AND sex = 'male') AS fever_male,
    COUNT(DISTINCT patient_id) FILTER(WHERE mrdt_result IN ('positive', 'negative') AND sex = 'female') AS mrdt_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE mrdt_result IN ('positive', 'negative') AND sex = 'male') AS mrdt_male,
    COUNT(DISTINCT patient_id) FILTER(WHERE mrdt_result = 'positive' AND sex = 'female') AS mrdt_positive_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE mrdt_result = 'positive' AND sex = 'male') AS mrdt_positive_male,
    COUNT(DISTINCT patient_id) FILTER(WHERE malaria_treatment = 'act' AND sex = 'female') AS received_act_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE malaria_treatment = 'act' AND sex = 'male') AS received_act_male,
    COUNT(DISTINCT patient_id) FILTER(WHERE NULLIF(danger_signs, '') IS NOT NULL AND patient_fever = 'yes' 
      AND sex = 'female') AS fever_danger_signs_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE NULLIF(danger_signs, '') IS NOT NULL AND patient_fever = 'yes' 
      AND sex = 'male') AS fever_danger_signs_male,
    COUNT(DISTINCT patient_id) FILTER(WHERE fever_duration = 1 AND sex = 'female') AS fever_treat_within_24_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE fever_duration = 1 AND sex = 'male') AS fever_treat_within_24_male,
    COUNT(DISTINCT patient_id) FILTER(WHERE diagnosis_cough ~~ 'pneumonia%' AND sex = 'female') AS pneumonia_dx_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE diagnosis_cough ~~ 'pneumonia%' AND sex = 'male') AS pneumonia_dx_male, 
    COUNT(DISTINCT patient_id) FILTER(WHERE pneumonia_treatment = 'amoxicillin' AND sex = 'female') AS received_amox_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE pneumonia_treatment = 'amoxicillin' AND sex = 'male') AS received_amox_male,
    COUNT(DISTINCT patient_id) FILTER(WHERE coughing_duration = 1 
      AND NULLIF(pneumonia_treatment, '') IS NOT NULL AND sex = 'female') AS pneumonia_treat_within_24_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE coughing_duration = 1 
      AND NULLIF(pneumonia_treatment, '') IS NOT NULL AND sex = 'male') AS pneumonia_treat_within_24_male,
    COUNT(DISTINCT patient_id) FILTER(WHERE referral_follow_up = 'true' and sex = 'female') AS referred_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE referral_follow_up = 'true' and sex = 'male') AS referred_male

  FROM
    {{ ref("useview_assessment") }}
  WHERE
    patient_age_in_months < 60 AND 
    patient_age_in_months >= 2 AND
    reported >= (date_trunc('day',starting_date))::date AND
    reported < (date_trunc('day',ending_date) + '1 day'::interval)::date  
  GROUP BY 
	area_uuid
) ASSESS ON chp.area_uuid = ASSESS.area_uuid

LEFT JOIN
(
  SELECT
    reported_by_parent  AS  area_uuid,
    COUNT(DISTINCT patient_id) FILTER(WHERE hf_visit = 'yes' AND sex = 'female') AS linked_to_care_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE hf_visit = 'yes' AND sex = 'male') AS linked_to_care_male   
  FROM
    {{ ref("formview_muac_follow_up") }}
  WHERE
    patient_age_in_months < 60 AND 
    patient_age_in_months >= 2 AND
    reported >= (date_trunc('day',starting_date))::date AND
    reported < (date_trunc('day',ending_date) + '1 day'::interval)::date  
  GROUP BY 
		area_uuid
) MUAC ON chp.area_uuid = MUAC.area_uuid

LEFT JOIN
(
  SELECT
    reported_by_parent AS area_uuid,
    COUNT(DISTINCT reported_by),
    COUNT(DISTINCT patient_id) FILTER(WHERE sex = 'female') AS fp_info_and_methods_female,
    COUNT(DISTINCT patient_id) FILTER(WHERE sex = 'male') AS fp_info_and_methods_male
  FROM
    {{ ref("formview_fp_patient_record") }}
  WHERE
		reported >= (date_trunc('day',starting_date))::date AND
    reported < (date_trunc('day',ending_date) + '1 day'::interval)::date
	GROUP BY
		area_uuid
) FP_RECORDS ON chp.area_uuid = FP_RECORDS.area_uuid

LEFT JOIN
(
  SELECT
    reported_by_parent AS area_uuid,
    COUNT(DISTINCT patient_id) FILTER(WHERE health_facility_delivery = 'no') AS home_deliveries
  FROM 
		{{ ref("useview_postnatal_care") }}
  WHERE
		reported >= (date_trunc('day',starting_date))::date AND
    reported < (date_trunc('day',ending_date) + '1 day'::interval)::date
	GROUP BY
		area_uuid
) PNC ON chp.area_uuid = PNC.area_uuid

LEFT JOIN
(
  WITH ANC_VISIT_CTE AS 
    (
      SELECT 
        area_uuid,
        "inputs/source_id",
        MAX(GREATEST(RIGHT(NULLIF(visit.anc_visit, ''),1)::int, up.anc_visit)) 
          OVER (PARTITION BY "inputs/source_id" ORDER BY visit.reported DESC) AS anc_visit
      FROM
        {{ ref("useview_pregnancy_visit") }} visit
      LEFT JOIN {{ ref("useview_pregnancy") }} up ON "inputs/source_id" = up.uuid
      WHERE
        visit.reported >= (date_trunc('day',starting_date))::date AND
        visit.reported < (date_trunc('day',ending_date) + '1 day'::interval)::date
    )

  SELECT 
    area_uuid,
    COUNT(DISTINCT "inputs/source_id") FILTER(WHERE anc_visit >= 4) AS atleast_4_anc,
    COUNT(DISTINCT "inputs/source_id") FILTER(WHERE anc_visit >= 8) AS atleast_8_anc
  FROM ANC_VISIT_CTE
  GROUP BY area_uuid
) ANC ON chp.area_uuid = ANC.area_uuid

LEFT JOIN
(
  SELECT
    area_uuid,
    COUNT(DISTINCT family_id) FILTER(WHERE hygeinic_toilet = 'yes') AS latrine,
    COUNT(DISTINCT family_id) FILTER(WHERE g_improved_latrine = 'yes') AS g_improved_latrine,
    COUNT(DISTINCT family_id) FILTER(WHERE g_handwashing_facility = 'yes') AS g_handwashing_facility,
    COUNT(DISTINCT family_id) FILTER(WHERE NULLIF(source_of_drinking_water, '') IS NOT NULL) AS source_of_drinking_water,
    COUNT(DISTINCT family_id) FILTER(WHERE g_open_defecation_free = 'yes') AS g_open_defecation_free
  FROM 
    {{ ref("useview_family_survey") }}
  WHERE
    reported >= (date_trunc('day',starting_date))::date AND
    reported < (date_trunc('day',ending_date) + '1 day'::interval)::date
  GROUP BY
    area_uuid
) FAMILY_SURVEY ON chp.area_uuid = FAMILY_SURVEY.area_uuid

LEFT JOIN
(
  SELECT
    reported_by_parent AS area_uuid,
    COUNT(DISTINCT fu.patient_id) FILTER(WHERE patient.sex = 'female' AND 
      (g_patient_treatment_outcome = 'cured' OR g_patient_referral_outcome = 'cured')) AS recovered_female,
    COUNT(DISTINCT fu.patient_id) FILTER(WHERE patient.sex = 'male' AND 
      (g_patient_treatment_outcome = 'cured' OR g_patient_referral_outcome = 'cured')) AS recovered_male
  FROM
    {{ ref("useview_assessment_follow_up") }} fu
  LEFT JOIN 
    {{ ref("contactview_person") }} patient ON patient.uuid = fu.patient_id
  WHERE
    patient_age_in_months < 60 AND
    patient_age_in_months >= 2 AND
    fu.reported >= (date_trunc('day',starting_date))::date AND
    fu.reported < (date_trunc('day',ending_date) + '1 day'::interval)::date  
  GROUP BY 
	area_uuid
) ASSESS_FOLLOWUP ON chp.area_uuid = ASSESS_FOLLOWUP.area_uuid

WHERE facility_name IS NOT NULL
GROUP BY
  facility_name,
  reporting_period,
  interval_start,
  interval_end
;
$function$
;