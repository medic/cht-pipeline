{{ config(materialized = 'raw_sql') }}  


CREATE OR REPLACE FUNCTION {{ this }} (group_by text, from_date timestamp with time zone, to_date timestamp with time zone, single_interval boolean)
RETURNS TABLE(
   branch_uuid text,
   branch_name text,
   supervisor_uuid text,
   supervisor_name text,
   chw_uuid text,
   chw_name text,
   chw_phone text,
   interval_start date,
   interval_number integer,
   fp_visits integer,
   visits_u15 integer,
   visits_15_19 integer,
   visits_20_24 integer,
   visits_25_49 integer,
   visits_50_plus integer,
   received_fp integer,
   received_fp_u15 integer,
   received_fp_15_19 integer,
   received_fp_20_24 integer,
   received_fp_25_49 integer,
   received_fp_50_plus integer,
   fp_referral_cases integer,
   fp_referrals_followed_up integer,
   referred_for_long_term_fp integer,
   referred_for_risk_factors integer,
   total_long_term_fp integer,
   total_iud integer,
   total_implant integer,
   total_depo_im integer,
   total_tubaligation integer,
   total_other integer,
   iud_cyp float,
   implant_cyp float,
   depo_im_cyp float,
   dmpa_sq_cyp float,
   cocs_cyp float,
   pops_cyp float,
   condoms_cyp float,
   ecp_cyp float,
   tl_cyp float,
   total_cyp float
)
 LANGUAGE sql
 STABLE
AS $function$

SELECT

  --CHW INFORMATION--
  CHWLIST.BRANCH_UUID AS _BRANCH_UUID,
  CHWLIST.BRANCH_NAME AS _BRANCH_NAME,

  CASE
    WHEN group_by = 'branch'
    THEN 'multiple'
    ELSE CHWLIST.SUPERVISOR_UUID
  END AS _SUPERVISOR_UUID,

  CASE
    WHEN group_by = 'branch'
    THEN 'multiple'
    ELSE CHWLIST.SUPERVISOR_NAME
  END AS _SUPERVISOR_NAME,

  CASE
    WHEN group_by = 'branch'
    THEN 'multiple'
    WHEN group_by = 'supervisor'
    THEN 'multiple'
    ELSE CHWLIST.CHW_UUID
  END AS _CHW_UUID,

  CASE
    WHEN group_by = 'branch'
    THEN 'multiple'
    WHEN group_by = 'supervisor'
    THEN 'multiple'
    ELSE CHWLIST.CHW_NAME
  END AS _CHW_NAME,

  CASE
    WHEN group_by = 'branch'
    THEN 'multiple'
    WHEN group_by = 'supervisor'
    THEN 'multiple'
    ELSE CHWLIST.CHW_PHONE
  END AS _CHW_PHONE,

  --TIME INTERVAL--
  date(CHWLIST.interval_start) AS _interval_start,
  CHWLIST.interval_number AS _interval_number,
  
  --FAMILY PLANNING--
  SUM(COALESCE(FPTOTALS.fp_visits, 0))::int AS fp_visits,
  SUM(COALESCE(FPTOTALS.visits_u15, 0))::int AS visits_u15,
  SUM(COALESCE(FPTOTALS.visits_15_19, 0))::int AS visits_15_19,
  SUM(COALESCE(FPTOTALS.visits_20_24, 0))::int AS visits_20_24,
  SUM(COALESCE(FPTOTALS.visits_25_49, 0))::int AS visits_25_49,
  SUM(COALESCE(FPTOTALS.visits_50_plus, 0))::int AS visits_50_plus,
  SUM(COALESCE(FPTOTALS.received_fp, 0))::int AS received_fp,
  SUM(COALESCE(FPTOTALS.received_fp_u15, 0))::int AS received_fp_u15,
  SUM(COALESCE(FPTOTALS.received_fp_15_19, 0))::int AS received_fp_15_19,
  SUM(COALESCE(FPTOTALS.received_fp_20_24, 0))::int AS received_fp_20_24,
  SUM(COALESCE(FPTOTALS.received_fp_25_49, 0))::int AS received_fp_25_49,
  SUM(COALESCE(FPTOTALS.received_fp_50_plus, 0))::int AS received_fp_50_plus,
  SUM(COALESCE(FPREFERRALS.count, 0))::int AS fp_referral_cases,
  SUM(COALESCE(FPREFERRALS.followed_up, 0))::int AS fp_referrals_followed_up,
  SUM(COALESCE(FPREFERRALS.referred_for_fp_method, 0))::int AS referred_for_long_term_fp,
  SUM(COALESCE(FPREFERRALS.referred_for_risks, 0))::int AS referred_for_risk_factors,
  SUM(COALESCE(FPREFERRALS.long_term_fp_given, 0))::int as total_long_term_fp,
  SUM(COALESCE(FPREFERRALS.iud, 0))::int AS total_iud,
  SUM(COALESCE(FPREFERRALS.implant, 0))::int AS total_implant,
  SUM(COALESCE(FPREFERRALS.depo_im, 0))::int AS total_depo_im,
  SUM(COALESCE(FPREFERRALS.tubaligation, 0))::int AS total_tubaligation,
  SUM(COALESCE(FPREFERRALS.other, 0))::int AS total_other,
  (SUM(COALESCE(FPTOTALS.iud, 0))::int * 4.6)::float AS iud_cyp,
  (SUM(COALESCE(FPTOTALS.implant, 0))::int * 3.8)::float AS implant_cyp,
  (SUM(COALESCE(FPTOTALS.depo_im, 0))::int * (1.0/4.0))::float AS depo_im_cyp,
  (SUM(COALESCE(FPTOTALS.dmpa_sq, 0))::int * (1.0/4.0))::float AS dmpa_sq_cyp,
  (SUM(COALESCE(FPTOTALS.cocs, 0))::int * (1.0/15.0))::float AS cocs_cyp,
  (SUM(COALESCE(FPTOTALS.pops, 0))::int * (1.0/15.0))::float AS pops_cyp,
  (SUM(COALESCE(FPTOTALS.condoms, 0))::int * (1.0/120.0))::float AS condoms_cyp,
  (SUM(COALESCE(FPTOTALS.ecp, 0))::int * (1.0/20.0))::float AS ecp_cyp,
  (SUM(COALESCE(FPTOTALS.tl, 0))::int * 10.0)::float AS tl_cyp,
  (
    (SUM(COALESCE(FPTOTALS.iud, 0))::int * 4.6) + -- 4.6 CYP per IUD inserted
    (SUM(COALESCE(FPTOTALS.implant, 0))::int * 3.8) + -- 3.8 CYP per implant
    (SUM(COALESCE(FPTOTALS.depo_im, 0))::int * (1.0/4.0)) + -- 4 doses per CYP
    (SUM(COALESCE(FPTOTALS.dmpa_sq, 0))::int * (1.0/4.0)) + -- 4 doses per CYP
    (SUM(COALESCE(FPTOTALS.cocs, 0))::int * (1.0/15.0)) + -- 15 cycles per CYP
    (SUM(COALESCE(FPTOTALS.pops, 0))::int * (1.0/15.0)) + -- 15 cycles per CYP
    (SUM(COALESCE(FPTOTALS.condoms, 0))::int * (1.0/120.0)) + -- 120 units per CYP
    (SUM(COALESCE(FPTOTALS.ecp, 0))::int * (1.0/20.0)) + -- 20 doses per CYP
    (SUM(COALESCE(FPTOTALS.tl, 0))::int * 10) -- 10 years
  )::float AS total_cyp


FROM
  (
    /*
      This query will provide a base list of CHPs/Branches regardless of whether or not they have created any
      data records.  This is used as the base of subsequent left joins.

      The data is grouped by Branch UUID and Name, CHP UUID and Name, Supervisor, and Month (for the current month and previous 3).
    */

    WITH periodCTE AS (
      SELECT
        interval_start::date AS interval_start,
        interval_number
      FROM (
        SELECT
          row_number() OVER (ORDER BY interval_start) as row_number,
          interval_start
        FROM generate_series(date_trunc('day',from_date), to_date, '1 month'::interval) AS interval_start
      ) AS dates
      INNER JOIN (
        SELECT
          row_number() OVER (ORDER BY interval_number) as row_number,
          interval_number
        FROM
          generate_series(0,(12*(extract (YEAR from age(to_date,from_date)))::int) + (extract (MONTH from age(to_date,from_date)))::int,1) AS interval_number
      ) AS intervals ON dates.row_number = intervals.row_number
      WHERE (CASE WHEN single_interval THEN dates.row_number = 1 ELSE dates.row_number >= 1 END)
    )

    SELECT
      chp.branch_uuid AS BRANCH_UUID,
      chp.branch_name AS BRANCH_NAME,
      chp.supervisor_uuid AS SUPERVISOR_UUID,
      cmeta.name AS SUPERVISOR_NAME,
      chp.area_uuid AS CHW_AREA_UUID,
      chp.uuid AS CHW_UUID,
      chp.name AS CHW_NAME,
      chp.phone AS CHW_PHONE,
      periodCTE.interval_start,
      periodCTE.interval_number

    FROM
      {{ ref("contactview_chp") }} chp
      INNER JOIN {{ ref("contactview_metadata") }} cmeta ON (cmeta.uuid = chp.supervisor_uuid)
      INNER JOIN {{ ref("contactview_metadata") }} cm ON (cm.contact_uuid = chp.uuid)
      CROSS JOIN periodCTE
    WHERE NOT EXISTS (SELECT NULL FROM {{ ref("get_muted_contacts") }}(to_date,'person') muted
          WHERE muted.contact_uuid = chp.uuid)

    GROUP BY
      chp.branch_uuid,
      chp.branch_name,
      chp.supervisor_uuid,
      chp.area_uuid,
      cmeta.name,
      chp.uuid,
      chp.name,
      chp.phone,
      interval_start,
      interval_number

  ) as CHWLIST

  LEFT JOIN
  (
    SELECT
      reported_by AS CHW_UUID,
      CASE
        WHEN single_interval
        THEN 0
        ELSE (12*(extract (YEAR FROM age(reported,from_date)))::int) + (extract (MONTH FROM age(reported,from_date)))::int
      END AS interval_number,
      SUM(new_fp_quantity) FILTER (WHERE fp_given = 'iud') AS iud,
      SUM(new_fp_quantity) FILTER (WHERE fp_given = 'implant') AS implant,
      SUM(new_fp_quantity) FILTER (WHERE fp_given = 'depo_im') AS depo_im,
      SUM(new_fp_quantity) FILTER (WHERE fp_given = 'tl') AS tl,
      SUM(new_fp_quantity) FILTER (WHERE fp_given = 'cocs') AS cocs,
      SUM(new_fp_quantity) FILTER (WHERE fp_given = 'pops') AS pops,
      SUM(new_fp_quantity) FILTER (WHERE fp_given = 'dmpa_sq') AS dmpa_sq,
      SUM(new_fp_quantity) FILTER (WHERE fp_given = 'condoms') AS condoms,
      SUM(new_fp_quantity) FILTER (WHERE fp_given = 'ecp') AS ecp,
      COUNT(uuid) AS fp_visits,
      COUNT(uuid) FILTER (WHERE patient_age_in_years < 15) AS visits_u15,
      COUNT(uuid) FILTER (WHERE patient_age_in_years BETWEEN 15 AND 19) AS visits_15_19,
      COUNT(uuid) FILTER (WHERE patient_age_in_years BETWEEN 20 AND 24) AS visits_20_24,
      COUNT(uuid) FILTER (WHERE patient_age_in_years BETWEEN 25 AND 49) AS visits_25_49,
      COUNT(uuid) FILTER (WHERE patient_age_in_years >= 50) AS visits_50_plus,
      COUNT(uuid) FILTER (WHERE fp_given IS NOT NULL) AS received_fp,
      COUNT(uuid) FILTER (WHERE fp_given IS NOT NULL AND patient_age_in_years < 15) AS received_fp_u15,
      COUNT(uuid) FILTER (WHERE fp_given IS NOT NULL AND patient_age_in_years BETWEEN 15 AND 19) AS received_fp_15_19,
      COUNT(uuid) FILTER (WHERE fp_given IS NOT NULL AND patient_age_in_years BETWEEN 20 AND 24) AS received_fp_20_24,
      COUNT(uuid) FILTER (WHERE fp_given IS NOT NULL AND patient_age_in_years BETWEEN 25 AND 49) AS received_fp_25_49,
      COUNT(uuid) FILTER (WHERE fp_given IS NOT NULL AND patient_age_in_years > 49) AS received_fp_50_plus
    FROM {{ ref("formview_fp_patient_record") }}
    WHERE 
      reported >= (date_trunc('day',from_date))::timestamp without time zone 
       AND reported < (date_trunc('day',to_date) + '1 day'::interval)::timestamp without time zone
    GROUP BY
      CHW_UUID,
       interval_number
  ) FPTOTALS ON (CHWLIST.CHW_UUID = FPTOTALS.CHW_UUID AND CHWLIST.interval_number = FPTOTALS.interval_number)
 
 LEFT JOIN
  (
    SELECT
      reported_by AS CHW_UUID,
      CASE
        WHEN single_interval
        THEN 0
        ELSE (12*(extract (YEAR FROM age(reported,from_date)))::int) + (extract (MONTH FROM age(reported,from_date)))::int
      END AS interval_number,
      COUNT(uuid) AS COUNT,
      COUNT(uuid) FILTER(WHERE referred_for_fp_method is TRUE) AS referred_for_fp_method,
      COUNT(uuid) FILTER(WHERE referred_for_risks is TRUE) AS referred_for_risks,
      COUNT(uuid) FILTER(WHERE followed_up IS TRUE) as followed_up,
      COUNT(uuid) FILTER(WHERE COALESCE(iud, implant, depo_im, tubaligation, other) IS NOT NULL) AS long_term_fp_given,
      COUNT(uuid) FILTER(WHERE iud IS TRUE) as iud,
      COUNT(uuid) FILTER(WHERE implant IS TRUE) as implant,
      COUNT(uuid) FILTER(WHERE depo_im IS TRUE) as depo_im,
      COUNT(uuid) FILTER(WHERE tubaligation IS TRUE) as tubaligation,
      COUNT(uuid) FILTER(WHERE other IS TRUE) as other
    FROM {{ ref("fp_referral_cases") }}
    WHERE 
      reported >= (date_trunc('day',from_date))::timestamp without time zone 
      AND  reported < (date_trunc('day',to_date) + '1 day'::interval)::timestamp without time zone
    GROUP BY
      CHW_UUID,
      interval_number
  ) AS FPREFERRALS ON (CHWLIST.CHW_UUID = FPREFERRALS.CHW_UUID AND CHWLIST.interval_number = FPREFERRALS.interval_number)

  GROUP BY
  _BRANCH_UUID,
  _BRANCH_NAME,
  _SUPERVISOR_UUID,
  _SUPERVISOR_NAME,
  _CHW_UUID,
  _CHW_NAME,
  _CHW_PHONE,
  _interval_start,
  _interval_number
;

$function$
;