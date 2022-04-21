{{ config(materialized = 'raw_sql') }}  


CREATE OR REPLACE FUNCTION {{ this }}(from_date timestamp with time zone, to_date timestamp with time zone)
RETURNS TABLE(
  branch_uuid text, 
  branch_name text, 
  supervisor_name text, 
  chw_uuid text, 
  chw_name text, 
  month text,
  assess_any int, 
  assess_u1 int, 
  assess_u5 int, 
  u1_malaria_treatment int,
  u1_diarrhea_treatment int,
  u1_pneumonia_treatment int,
  treatments_u1 int, 
  malaria_u1 int, 
  diarrhea_u1 int, 
  pneumonia_u1 int, 
  u5_malaria_treatment int,
  u5_diarrhea_treatment int,
  u5_pneumonia_treatment int,
  treatments_u5 int, 
  malaria_u5 int, 
  diarrhea_u5 int, 
  pneumonia_u5 int, 
  mrdt_positive int, 
  mrdt_negative int, 
  mrdt_none int, 
  percent_mrdt float, 
  required_follow_ups int,
  on_time_follow_ups int, 
    missed_visits int, 
    on_time_follow_up_percent float 
) AS

$BODY$
SELECT 
    CHWLIST.BRANCH_UUID, 
    CHWLIST.BRANCH_NAME, 
    CHWLIST.SUPERVISOR_NAME,
    CHWLIST.CHW_UUID, 
    CHWLIST.CHW_NAME, 
    CHWLIST.MONTH, 
    
      --ASSESSMENTS-
  COALESCE(ASSESS.assess_any,0)::int AS  assess_any,
  COALESCE(ASSESS.assess_u1,0)::int AS assess_u1,
  COALESCE(ASSESS.assess_u5,0)::int AS assess_u5,
  
  --TREATMENTS U1--
  COALESCE(ASSESS.u1_malaria_treatment,0)::int AS u1_malaria_treatment,
  COALESCE(ASSESS.u1_diarrhea_treatment,0)::int AS u1_diarrhea_treatment,
  COALESCE(ASSESS.u1_pneumonia_treatment,0)::int AS u1_pneumonia_treatment,
  (COALESCE(ASSESS.u1_malaria_treatment,0) + COALESCE(ASSESS.u1_diarrhea_treatment,0) + COALESCE(ASSESS.u1_pneumonia_treatment,0))::int AS treatments_u1,
  
  COALESCE(ASSESS.malaria_u1,0)::int AS malaria_u1,
  COALESCE(ASSESS.diarrhea_u1,0)::int AS diarrhea_u1,
  COALESCE(ASSESS.pneumonia_u1,0)::int AS pneumonia_u1,
  
  --TREATMENTs U5--
  COALESCE(ASSESS.u5_malaria_treatment,0)::int AS u5_malaria_treatment,
  COALESCE(ASSESS.u5_diarrhea_treatment,0)::int AS u5_diarrhea_treatment,
  COALESCE(ASSESS.u5_pneumonia_treatment,0)::int AS u5_pneumonia_treatment,
  (COALESCE(ASSESS.u5_malaria_treatment,0) + COALESCE(ASSESS.u5_diarrhea_treatment,0) + COALESCE(ASSESS.u5_pneumonia_treatment,0))::int AS treatments_u5,
  
  COALESCE(ASSESS.malaria_u5,0)::int AS malaria_u5,
  COALESCE(ASSESS.diarrhea_u5,0)::int AS diarrhea_u5,
  COALESCE(ASSESS.pneumonia_u5,0)::int AS pneumonia_u5,
  
  --mRDT--
  COALESCE(ASSESS.mrdt_positive,0)::int AS mrdt_positive,
  COALESCE(ASSESS.mrdt_negative,0)::int AS mrdt_negative,
  COALESCE(ASSESS.mrdt_none,0)::int AS mrdt_none,
  CASE
    WHEN (COALESCE(ASSESS.mrdt_positive,0) + COALESCE(ASSESS.mrdt_negative,0) + COALESCE(ASSESS.mrdt_none,0)) = 0
    THEN 0::float
    ELSE
      (COALESCE(ASSESS.mrdt_positive,0) + COALESCE(ASSESS.mrdt_negative,0))::float / (COALESCE(ASSESS.mrdt_positive,0) + COALESCE(ASSESS.mrdt_negative,0) + COALESCE(ASSESS.mrdt_none,0))::float
  END AS percent_mrdt,
  
  --TREATMENT FOLLOW UPS--
  COALESCE(ASSESS.required_follow_ups,0)::int AS required_follow_ups,
  COALESCE(ON_TIME_FOLLOW_UPS.COUNT,0)::int AS on_time_follow_ups,
  (COALESCE(ASSESS.required_follow_ups,0)::int - COALESCE(ON_TIME_FOLLOW_UPS.COUNT,0)::int)::int AS missed_visits,
  CASE
    WHEN COALESCE(ASSESS.required_follow_ups,0) = 0
    THEN 0::float
    ELSE
      COALESCE(ON_TIME_FOLLOW_UPS.COUNT,0)::float / COALESCE(ASSESS.required_follow_ups,0)::float     
  END AS on_time_follow_up_percent
FROM 
    (
        SELECT 
            chp.uuid AS CHW_UUID, 
            chp.name AS CHW_NAME, 
            branch.uuid AS BRANCH_UUID, 
            branch.name AS BRANCH_NAME, 
            to_char(
                assess.reported, 'YYYYMM'
            ) AS MONTH,
            cmeta.name AS SUPERVISOR_NAME 
        FROM 
            useview_assessment assess
            INNER JOIN contactview_chp chp ON assess.chw = chp.uuid
            INNER JOIN contactview_branch branch ON chp.branch_uuid = branch.uuid 
            INNER JOIN contactview_metadata cmeta ON chp.supervisor_uuid = cmeta.uuid 
        WHERE 
       
            branch.name != 'HQ' 
            AND branch.name != 'HQ OVC' 
            AND assess.reported >= (date_trunc('day',from_date))::timestamp without time zone
      AND assess.reported < (date_trunc('day',to_date) + '1 day'::interval)::timestamp without time zone
        GROUP BY 
            chp.name, 
            branch.name, 
            chp.uuid, 
            branch.uuid, 
            MONTH, 
            supervisor_name
    ) AS CHWLIST 
    LEFT JOIN (
      SELECT
          chw AS CHW_UUID,
          to_char(useview_assessment.reported, 'YYYYMM') AS MONTH,
          count(*) AS assess_any,
      
          sum(CASE 
              WHEN
                (patient_age_in_years)::int < 1
                THEN 1 
              ELSE
                0
            END) AS assess_u1,
          sum(CASE 
              WHEN
                (patient_age_in_years)::int < 5
                THEN 1 
              ELSE
                0
            END) AS assess_u5,   
       
          --Malaria All Ages--
          sum(CASE 
              WHEN
                (diagnosis_fever) like 'malaria%'
                THEN 1 
              ELSE
                0
            END) AS malaria_all_ages,      
       
          sum(CASE 
              WHEN
                (patient_age_in_years)::int < 1 AND
                (diagnosis_fever) like 'malaria%'
                THEN 1 
              ELSE
                0
            END) AS malaria_u1,
          sum(CASE 
              WHEN
                (patient_age_in_years)::int < 1 AND
                (diagnosis_diarrhea) like 'diarrhea%'
                THEN 1 
              ELSE
                0
            END) AS diarrhea_u1,
          sum(CASE 
              WHEN
                (patient_age_in_years)::int < 1 AND
                (diagnosis_cough) like 'pneumonia%'
                THEN 1 
              ELSE
                0
            END) AS pneumonia_u1,  
          sum(CASE 
              WHEN
                (patient_age_in_years)::int < 5 AND
                (diagnosis_fever) like 'malaria%'
                THEN 1 
              ELSE
                0
            END) AS malaria_u5,
          sum(CASE 
              WHEN
                (patient_age_in_years)::int < 5 AND
                (diagnosis_diarrhea) like 'diarrhea%'
                THEN 1 
              ELSE
                0
            END) AS diarrhea_u5,
          sum(CASE 
              WHEN
                (patient_age_in_years)::int < 5 AND
                (diagnosis_cough) like 'pneumonia%'
                THEN 1 
              ELSE
                0
            END) AS pneumonia_u5,

          --U1 TREATMENTS--
          COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 1 AND diarrhea_treatment IS NOT NULL) AS u1_diarrhea_treatment,
          COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 1 AND pneumonia_treatment IS NOT NULL) AS u1_pneumonia_treatment,
          COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 1 AND malaria_treatment IS NOT NULL) AS u1_malaria_treatment,

          --U5 TREATMENTS--
          COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 5 AND diarrhea_treatment IS NOT NULL) AS u5_diarrhea_treatment,
          COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 5 AND pneumonia_treatment IS NOT NULL) AS u5_pneumonia_treatment,
          COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 5 AND malaria_treatment IS NOT NULL) AS u5_malaria_treatment,
    
          --mRDT--
          sum(CASE 
              WHEN
                (patient_fever) = 'yes' AND
                (mrdt_result) = 'positive'
                THEN 1 
              ELSE
                0
            END) AS mrdt_positive,
          sum(CASE 
              WHEN
                (patient_fever) = 'yes' AND
                (mrdt_result) = 'negative'
                THEN 1 
              ELSE
                0
            END) AS mrdt_negative,
          sum(CASE 
              WHEN
                (patient_fever) = 'yes' AND
                (mrdt_result) = 'none'
                THEN 1 
              ELSE
                0
            END) AS mrdt_none,
    
          --REQUIRED FOLLOW UPS--
          sum(CASE 
              WHEN
                (patient_age_in_years)::int < 5 AND
                (referral_follow_up) = 'true'
                THEN 1 
              ELSE
                0
            END) AS required_follow_ups   
        FROM 
            useview_assessment
        WHERE 
            useview_assessment.reported >= (date_trunc('day',from_date))::timestamp without time zone AND
      useview_assessment.reported < (date_trunc('day',to_date) + '1 day'::interval)::timestamp without time zone
        GROUP BY 
            useview_assessment.chw, 
            MONTH
    ) AS ASSESS ON ASSESS.CHW_UUID = CHWLIST.CHW_UUID 
    AND ASSESS.MONTH = CHWLIST.MONTH
    LEFT JOIN
  
    (
      SELECT
        assess.chw as CHW_UUID,
        to_char(assess.reported, 'YYYYMM') AS MONTH,
        count(assess.UUID) as count
    
      FROM
        useview_assessment assess
        INNER JOIN formview_assessment_follow_up assess_fu ON (assess.UUID = assess_fu.form_source_id)
        INNER JOIN form_metadata fu_meta ON (fu_meta.uuid = assess_fu.xmlforms_uuid)
    
      WHERE
        assess.reported >= (date_trunc('day',from_date))::timestamp without time zone AND
        assess.reported < (date_trunc('day',to_date) + '1 day'::interval)::timestamp without time zone AND
        assess.referral_follow_up = 'true' AND
        assess.patient_age_in_years::int < 5 AND
        assess_fu.follow_up_count = '1' AND
        (date(fu_meta.reported) - date(assess.reported)) <= 1
    
      GROUP BY
        CHW_UUID,
        Month   
    
    ) AS ON_TIME_FOLLOW_UPS ON (CHWLIST.CHW_UUID = ON_TIME_FOLLOW_UPS.CHW_UUID AND CHWLIST.MONTH = ON_TIME_FOLLOW_UPS.MONTH)
$BODY$
LANGUAGE 'sql' STABLE;
	