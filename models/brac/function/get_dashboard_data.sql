{{ config(materialized = 'raw_sql') }}


CREATE OR REPLACE FUNCTION {{ this }}(group_by text, from_date timestamp with time zone, to_date timestamp with time zone, single_interval boolean)
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
   active_chws_in_period integer,
   families_registered integer,
   pregnancies_registered integer,
   fp_visits integer,
   fp_referral_cases integer,
   fp_referrals_followed_up integer,
   total_cyp float,
   on_time_pnc_visits integer,
   total_healthy_pnc_visits integer,
   all_first_fu_pnc_visits integer,
   edd_no_pnc integer,
   assess_any integer,
   assess_u1 integer,
   assess_u5 integer,
   treatments_u1 integer,
   malaria_u1 integer,
   diarrhea_u1 integer,
   pneumonia_u1 integer,
   u5_malaria_treatment integer,
   u5_diarrhea_treatment integer,
   u5_pneumonia_treatment integer,
   treatments_u5 integer,
   malaria_u5 integer,
   diarrhea_u5 integer,
   pneumonia_u5 integer,
   mrdt_positive integer,
   mrdt_negative integer,
   mrdt_none integer,
   mrdt_chp integer,
   mrdt_other integer,
   percent_mrdt double precision,
   required_follow_ups integer,
   on_time_follow_ups integer,
   missed_visits integer,
   on_time_follow_up_percent double precision,
   families_registered_all_time integer,
   hh_visits integer,
   hh_visit_percent double precision,
   malaria_all_ages integer,
   chws_registering_families_all_time integer,
   community_events integer,
   active_chws_in_range integer,
   family_surveys integer,
   covid_referrals integer,
   covid_tested_positive integer,
   total_covid_hbc integer,
   covid_patients_evacuated integer,
   covid_patients_deisolated integer,
   covid_patients_dead integer,
   covid_hh_others_sick integer,
   hh_with_covid integer
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

  --ACTIVE?--
  sum(
    CASE
      WHEN
        (COALESCE(ALLPREG.COUNT,0)::int + COALESCE(ASSESS.assess_any,0)::int + COALESCE(PNCVISIT.ALL_FU1_PNC,0)::int) > 0
        THEN 1
      ELSE
        0
    END)::int AS ACTIVE_CHWS_IN_PERIOD,


  --FAMILIES--
  sum(COALESCE(FAMILYREG.COUNT,0))::int AS families_registered,

  --PREGNANCY--
  sum(COALESCE(ALLPREG.COUNT,0))::int AS pregnancies_registered,

  --FAMILY PLANNING--
  SUM(COALESCE(FPTOTALS.fp_visits, 0))::int AS fp_visits,
  SUM(COALESCE(FPREFERRALS.count, 0))::int AS fp_referral_cases,
  SUM(COALESCE(FPREFERRALS.followed_up, 0))::int AS fp_referrals_followed_up,
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
  )::float AS total_cyp,

  --POSTNATAL CARE--
  sum(COALESCE(PNCVISIT.ON_TIME_PNC,0))::int AS on_time_pnc_visits,
  sum(COALESCE(PNCVISIT.HEALTHY_PNC,0))::int AS total_healthy_pnc_visits,
  sum(COALESCE(PNCVISIT.ALL_FU1_PNC,0))::int AS all_first_fu_pnc_visits,
  sum(COALESCE(EDDNOPNC.COUNT,0))::int AS edd_no_pnc,

  --ASSESSMENTS-
  sum(COALESCE(ASSESS.assess_any,0))::int AS assess_any,
  sum(COALESCE(ASSESS.assess_u1,0))::int AS assess_u1,
  sum(COALESCE(ASSESS.assess_u5,0))::int AS assess_u5,

  --TREATMENTS--
  sum(COALESCE(ASSESS.u1_malaria_treatment,0) + COALESCE(ASSESS.u1_diarrhea_treatment,0) + COALESCE(ASSESS.u1_pneumonia_treatment,0))::int AS treatments_u1,

  sum(COALESCE(ASSESS.malaria_u1,0))::int AS malaria_u1,
  sum(COALESCE(ASSESS.diarrhea_u1,0))::int AS diarrhea_u1,
  sum(COALESCE(ASSESS.pneumonia_u1,0))::int AS pneumonia_u1,

  sum(COALESCE(ASSESS.u5_malaria_treatment,0))::int AS u5_malaria_treatment,
  sum(COALESCE(ASSESS.u5_diarrhea_treatment,0))::int AS u5_diarrhea_treatment,
  sum(COALESCE(ASSESS.u5_pneumonia_treatment,0))::int AS u5_pneumonia_treatment,
  sum(COALESCE(ASSESS.u5_malaria_treatment,0) + COALESCE(ASSESS.u5_diarrhea_treatment,0) + COALESCE(ASSESS.u5_pneumonia_treatment,0))::int AS treatments_u5,

  sum(COALESCE(ASSESS.malaria_u5,0))::int AS malaria_u5,
  sum(COALESCE(ASSESS.diarrhea_u5,0))::int AS diarrhea_u5,
  sum(COALESCE(ASSESS.pneumonia_u5,0))::int AS pneumonia_u5,

  --mRDT--
  sum(COALESCE(ASSESS.mrdt_positive,0))::int AS mrdt_positive,
  sum(COALESCE(ASSESS.mrdt_negative,0))::int AS mrdt_negative,
  sum(COALESCE(ASSESS.mrdt_none,0))::int AS mrdt_none,
  sum(COALESCE(ASSESS.mrdt_chp,0))::int AS mrdt_chp,
  sum(COALESCE(ASSESS.mrdt_other,0))::int AS mrdt_other,
  CASE
    WHEN sum(COALESCE(ASSESS.mrdt_positive,0) + COALESCE(ASSESS.mrdt_negative,0) + COALESCE(ASSESS.mrdt_none,0))::int = 0
    THEN 0::float
    ELSE
      sum(COALESCE(ASSESS.mrdt_positive,0) + COALESCE(ASSESS.mrdt_negative,0))::float / sum(COALESCE(ASSESS.mrdt_positive,0) + COALESCE(ASSESS.mrdt_negative,0) + COALESCE(ASSESS.mrdt_none,0))::float
  END AS percent_mrdt,

  --TREATMENT FOLLOW UPS--
  sum(COALESCE(ASSESS.required_follow_ups,0))::int AS required_follow_ups,
  sum(COALESCE(ON_TIME_FOLLOW_UPS.COUNT,0))::int AS on_time_follow_ups,
  sum(COALESCE(ASSESS.required_follow_ups,0)::int - COALESCE(ON_TIME_FOLLOW_UPS.COUNT,0)::int)::int AS missed_visits,
  CASE
    WHEN sum(COALESCE(ASSESS.required_follow_ups,0)) = 0
    THEN 0::float
    ELSE
      sum(COALESCE(ON_TIME_FOLLOW_UPS.COUNT,0))::float / sum(COALESCE(ASSESS.required_follow_ups,0))::float
  END AS on_time_follow_up_percent,

  --CUMMULATIVE NUMBER OF FAMILIES REGISTERED ALL TIME--
  sum(COALESCE(FAMILYREGTOTAL.COUNT,0))::int AS families_registered_all_time,

  --HH visits--
  SUM(COALESCE(HH_VISITS.COUNT, 0))::int AS hh_visits,
  CASE
    WHEN sum(COALESCE(FAMILYREGTOTAL.COUNT,0)) = 0
    THEN 0::float
    ELSE
      SUM(COALESCE(HH_VISITS.COUNT, 0))::float / sum(COALESCE(FAMILYREGTOTAL.COUNT,0))::float
  END AS hh_visit_percent,

  --Number of all age malaria treatments--
  sum(COALESCE(ASSESS.malaria_all_ages,0))::int AS malaria_all_ages,

  --NUMBER OF CHWS REGISTERD FAMILIES ALL TIME--
  count(distinct(FAMILYREGTOTAL.CHW_AREA_UUID))::int AS chws_registering_families_all_time,

  --Number of Community Event forms submitted in the given period--
  sum(COALESCE(COMMUNITYEVENT.COUNT,0))::int AS community_events,

  --Whether or not CHW was active within the entire date range--
  sum(COALESCE(ACTIVE_IN_RANGE.ACTIVE,0))::int AS active_chws_in_range,

  --Number of family surveys conducted within range--
  sum(COALESCE(FAMILY_SURVEY.COUNT,0))::int AS family_surveys,

  --COVID-19
  sum(COALESCE(COVID.COUNT,0))::int AS covid_referrals,
  sum(COALESCE(COVID.tested_positive,0))::int AS covid_tested_positive,
  sum(COALESCE(COVID.total_in_hbc,0))::int AS total_covid_hbc,
  sum(COALESCE(COVID.patients_evacuated,0))::int AS covid_patients_evacuated,
  sum(COALESCE(COVID.patients_deisolated,0))::int AS covid_patients_deisolated,
  sum(COALESCE(COVID.patients_dead,0))::int AS covid_patients_dead,
  sum(COALESCE(COVID.hh_others_sick,0))::int AS covid_hh_others_sick,
  sum(COALESCE(COVID.hh_with_covid,0))::int AS hh_with_covid


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
        parent_uuid AS CHW_AREA_UUID,
        CASE
          WHEN single_interval
          THEN 0
          ELSE (12*(extract (YEAR from age(reported,from_date)))::int) + (extract (MONTH from age(reported,from_date)))::int
        END AS interval_number,
        count(uuid) AS COUNT

      FROM
        {{ ref("contactview_metadata") }}

      WHERE
        type = 'clinic' AND
        reported >= (date_trunc('day',from_date))::timestamp without time zone AND
        reported < (date_trunc('day',to_date) + '1 day'::interval)::timestamp without time zone
      GROUP BY
        CHW_AREA_UUID,
        interval_number

    ) AS FAMILYREG ON (CHWLIST.CHW_AREA_UUID = FAMILYREG.CHW_AREA_UUID AND CHWLIST.interval_number = FAMILYREG.interval_number)

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
      COUNT(uuid) FILTER(WHERE followed_up IS TRUE) as followed_up
    FROM {{ ref("fp_referral_cases") }}
    WHERE
      reported >= (date_trunc('day',from_date))::timestamp without time zone
      AND  reported < (date_trunc('day',to_date) + '1 day'::interval)::timestamp without time zone
    GROUP BY
      CHW_UUID,
      interval_number
  ) AS FPREFERRALS ON (CHWLIST.CHW_UUID = FPREFERRALS.CHW_UUID AND CHWLIST.interval_number = FPREFERRALS.interval_number)

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
      COUNT(uuid) AS fp_visits
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
            referral.reported_by_parent AS CHW_AREA_UUID,
            CASE
                WHEN single_interval
                THEN 0
                ELSE (12*(extract (YEAR from age(referral.reported,from_date)))::int) + (extract (MONTH from age(referral.reported, to_date)))::int
            END AS interval_number,
            count(referral.uuid) AS COUNT,
            count(referral.uuid) FILTER (WHERE referral.followup_count = 1 AND referral.test_result = 'positive') AS tested_positive,
            count(referral.uuid) FILTER (WHERE referral.followup_count = 1 AND referral.in_hbc = 'true') AS total_in_hbc,
            count(referral.uuid) FILTER (WHERE evacuation.patient_evacuated = 'yes') AS patients_evacuated,
            count(referral.uuid) FILTER (WHERE patient_deisolated = 'yes') AS patients_deisolated,
            count(referral.uuid) FILTER (WHERE reason_not_available = 'dead') AS patients_dead,
            count(DISTINCT referral.parent_id) FILTER (WHERE hh_contact = 'yes') AS hh_others_sick,
            count(DISTINCT referral.parent_id) AS hh_with_covid
        FROM
            {{ ref("formview_covid_referral_follow_up") }} referral
        LEFT JOIN {{ ref("formview_covid_evacuation_follow_up") }} evacuation ON evacuation.source_id = referral.uuid
        WHERE
            referral.reported >= (date_trunc('day',from_date))::timestamp without time zone AND
			referral.reported < (date_trunc('day',to_date) + '1 day'::interval)::timestamp without time zone
        GROUP BY
            CHW_AREA_UUID,
            interval_number
    ) AS COVID ON (CHWLIST.CHW_AREA_UUID = COVID.CHW_AREA_UUID AND CHWLIST.interval_number = COVID.interval_number)

  LEFT JOIN

    (
      /*
        This query will provide the total number pregnancies registered in the given period.
      */
      SELECT
        meta.reported_by AS CHW_UUID,
        CASE
          WHEN single_interval
          THEN 0
          ELSE (12*(extract (YEAR from age(meta.reported,from_date)))::int) + (extract (MONTH from age(meta.reported,from_date)))::int
        END AS interval_number,

        count(meta.uuid) AS COUNT

      FROM
        {{ ref("form_metadata") }} meta
      INNER JOIN
        (
          SELECT
            DISTINCT ON (patient_id, date_trunc('day',reported)) uuid,preg_test
          FROM {{ ref("useview_pregnancy") }}
          WHERE preg_test != 'neg' /* only select positive pregnancies */
        ) up ON (up.uuid = meta.uuid)

      WHERE
        meta.form = 'pregnancy' AND
        meta.reported >= (date_trunc('day',from_date))::timestamp without time zone AND
        meta.reported < (date_trunc('day',to_date) + '1 day'::interval)::timestamp without time zone

      GROUP BY
        CHW_UUID,
        interval_number

    ) as ALLPREG ON (CHWLIST.CHW_UUID = ALLPREG.CHW_UUID AND CHWLIST.interval_number = ALLPREG.interval_number)

  LEFT JOIN

    (
      /*
        This query will provide the total number of PNC visits where the outcome was not miscarriage or still_birth
        and also the number of 'on time visits' as defined by two days between reported date and actual delivery date.

        This will be joined to the CHPLIST query by the UUID of the CHP
      */
      SELECT
        pnc.chw AS CHW_UUID,
        CASE
          WHEN single_interval
          THEN 0
          ELSE (12*(extract (YEAR from age(reported,from_date)))::int) + (extract (MONTH from age(reported,from_date)))::int
        END AS interval_number,

        sum(
            CASE
              WHEN
                pnc.pregnancy_outcome <> 'miscarriage' AND
                pnc.pregnancy_outcome <> 'still_birth' AND
                (date_trunc('day',pnc.reported) - date(pnc.delivery_date)) < '4 days'::interval
                THEN 1
              ELSE
                0
            END
          ) AS ON_TIME_PNC,

        sum(
            CASE
              WHEN
                pnc.pregnancy_outcome <> 'miscarriage' AND
                pnc.pregnancy_outcome <> 'still_birth'
                THEN 1
              ELSE
                0
            END

          ) AS HEALTHY_PNC,

        count(pnc.UUID) AS ALL_FU1_PNC

      FROM
        {{ ref("useview_postnatal_care") }} pnc

      WHERE
        pnc.follow_up_count = '1' AND
        pnc.reported >= (date_trunc('day',from_date))::timestamp without time zone AND
        pnc.reported < (date_trunc('day',to_date) + '1 day'::interval)::timestamp without time zone

      GROUP BY
        CHW_UUID,
        interval_number

    ) as PNCVISIT ON CHWLIST.CHW_UUID = PNCVISIT.CHW_UUID AND CHWLIST.interval_number = PNCVISIT.interval_number

  LEFT JOIN

    (

      /*
        The mostRecentPregnancy subquery gets us the EDD for the most recent pregnancy for all patients.  This
        outer query, patientsWithEDDInRange gives us only the EDDs where the EDD for the most recent pregnancy is
        in the desired date range.
      */
      SELECT
        MOSTRECENTPREG.CHW_UUID as CHW_UUID,

        CASE
          WHEN single_interval
          THEN 0
          ELSE (12*(extract (YEAR from age(MOSTRECENTPREG.EDD,from_date)))::int) + (extract (MONTH from age(MOSTRECENTPREG.EDD,from_date)))::int
        END AS interval_number,

        count(MOSTRECENTPREG.PATIENT_ID) as COUNT

      FROM
        (
            /*
              This query gives us a list of EDDs for the most recent pregnancy for each patient where the EDD is in
              the desired date range.
            */
            SELECT
              chw AS CHW_UUID,
              patient_id AS patient_id,
              max(date(edd)) AS EDD

            FROM
               {{ ref("useview_pregnancy") }}

            GROUP BY
              CHW_UUID,
              PATIENT_ID

            HAVING
              max(date(edd)) >= (date_trunc('day',from_date)) AND

              CASE
                WHEN date_trunc('day',to_date) = date_trunc('day',now())
                THEN max(date(edd)) <= (to_date - '3 days'::interval)
                ELSE max(date(edd)) <= (to_date + '1 day'::interval)
              END

        ) AS MOSTRECENTPREG

        LEFT JOIN
        (
          /*
            This query gives us a list of Reported Dates for the most recent delivery for each patient.
            Ultimately we are looking for Pregnancies that don't have a delivery notification so we are
            not excluding miscarriages and still_births from this query.
          */
          SELECT
            pnc.patient_id,
            max(pnc.reported) as maxReported

          FROM
            {{ ref("useview_postnatal_care") }} pnc

          WHERE
            pnc.follow_up_count = '1'

          GROUP BY
            pnc.patient_id

        ) AS MOSTRECENTDELIV ON (MOSTRECENTPREG.patient_id = MOSTRECENTDELIV.patient_id)

      WHERE
        maxReported is null OR
        @(EDD::date - maxReported::date) > 60

      GROUP BY
        MOSTRECENTPREG.CHW_UUID,
        interval_number

    ) AS EDDNOPNC ON CHWLIST.CHW_UUID = EDDNOPNC.CHW_UUID AND CHWLIST.interval_number = EDDNOPNC.interval_number

  LEFT JOIN

    (
      WITH ASSESSMENTS AS (
        SELECT DISTINCT ON(patient_id, date_trunc('day',reported))
          *
        FROM {{ ref("useview_assessment") }}
        WHERE
            patient_age_in_months >= 2 AND
            reported >= (date_trunc('day',from_date))::timestamp without time zone AND
            reported < (date_trunc('day',to_date) + '1 day'::interval)::timestamp without time zone
      )
            SELECT
          chw AS CHW_UUID,

          CASE
            WHEN single_interval
            THEN 0
            ELSE (12*(extract (YEAR from age(date_trunc('day',reported),from_date)))::int) + (extract (MONTH from age(date_trunc('day',reported),from_date)))::int
          END AS interval_number,

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

        --U1 TREATMENTS--
        COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 1 AND diarrhea_treatment IS NOT NULL) AS u1_diarrhea_treatment,
        COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 1 AND pneumonia_treatment IS NOT NULL) AS u1_pneumonia_treatment,
        COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 1 AND malaria_treatment IS NOT NULL) AS u1_malaria_treatment,

        --U5 TREATMENTS--
        COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 5 AND diarrhea_treatment IS NOT NULL) AS u5_diarrhea_treatment,
        COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 5 AND pneumonia_treatment IS NOT NULL) AS u5_pneumonia_treatment,
        COUNT(uuid) FILTER (WHERE patient_age_in_years::int < 5 AND malaria_treatment IS NOT NULL) AS u5_malaria_treatment,

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
          sum(CASE
              WHEN
                (patient_fever) = 'yes' AND
                (mrdt_source) = 'chp'
                THEN 1
              ELSE
                0
            END) AS mrdt_chp,
          sum(CASE
              WHEN
                (patient_fever) = 'yes' AND
                (mrdt_source) = 'other'
                THEN 1
              ELSE
                0
            END) AS mrdt_other,

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
            ASSESSMENTS

          GROUP BY
            CHW_UUID,
            interval_number

    ) AS ASSESS ON (CHWLIST.CHW_UUID = ASSESS.CHW_UUID AND CHWLIST.interval_number = ASSESS.interval_number)

  LEFT JOIN

    (
      SELECT
        assess.chw as CHW_UUID,
        CASE
          WHEN single_interval
          THEN 0
          ELSE (12*(extract (YEAR from age(assess.reported,from_date)))::int) + (extract (MONTH from age(assess.reported,from_date)))::int
        END AS interval_number,

        count(assess.uuid) as count

      FROM
        {{ ref("useview_assessment") }} AS assess
        INNER JOIN  (
          SELECT
            DISTINCT ON (form_source_id, day)
            uuid,
            form_source_id,
            reported
          FROM(

            SELECT
              uuid,
              form_source_id,
              reported,
              date_trunc('day', follow_up.reported) as day
            FROM
              {{ ref("useview_assessment_follow_up") }} AS follow_up
            ORDER BY form_source_id, reported
          ) AS base
        ) AS follow_up ON (assess.uuid = follow_up.form_source_id)
      WHERE
        assess.reported >= (date_trunc('day',from_date))::timestamp without time zone AND
        assess.reported < (date_trunc('day',to_date) + '1 day'::interval)::timestamp without time zone AND
        assess.referral_follow_up = 'true' AND
        assess.patient_age_in_years::int < 5 AND
        date(date_trunc('day',follow_up.reported)) - date(date_trunc('day',assess.reported)) <= 2

      GROUP BY
        CHW_UUID,
        interval_number

    ) AS ON_TIME_FOLLOW_UPS ON (CHWLIST.CHW_UUID = ON_TIME_FOLLOW_UPS.CHW_UUID AND CHWLIST.interval_number = ON_TIME_FOLLOW_UPS.interval_number)

    LEFT JOIN

    (
      SELECT
        parent_uuid AS CHW_AREA_UUID,
        0 AS interval_number,
        count(*) AS COUNT

      FROM
        {{ ref("contactview_metadata") }} family

      WHERE
        type = 'clinic'
        AND NOT EXISTS (SELECT NULL FROM {{ ref("get_muted_contacts") }}(to_date,'clinic') muted
          WHERE muted.contact_uuid = family.uuid)

      GROUP BY
        CHW_AREA_UUID,
        interval_number

    ) AS FAMILYREGTOTAL ON (CHWLIST.CHW_AREA_UUID = FAMILYREGTOTAL.CHW_AREA_UUID AND CHWLIST.interval_number = FAMILYREGTOTAL.interval_number)

  LEFT JOIN
  (
    WITH VISITS_CTE AS(
      SELECT
        record.reported,
        patient.parent_uuid AS place_id,
        reported_by
      FROM
        {{ ref("useview_patient_record") }} record
      LEFT JOIN
        {{ ref("contactview_metadata") }} patient ON patient.uuid = patient_id
      WHERE
        record.reported >= (date_trunc('day',from_date))::timestamp without time zone AND
        record.reported < (date_trunc('day',to_date) + '1 day'::interval)::timestamp without time zone AND
        form NOT IN('mute', 'unmute')
      UNION ALL
      SELECT
        reported,
        place_id,
        reported_by
      FROM
	      {{ ref("useview_place_record") }}
      WHERE
        reported >= (date_trunc('day',from_date))::timestamp without time zone AND
        reported < (date_trunc('day',to_date) + '1 day'::interval)::timestamp without time zone AND
		    place_id != '' AND place_id IS NOT NULL
    )
    SELECT
      visit.reported_by AS CHW_UUID,
      CASE
        WHEN single_interval
        THEN 0
        ELSE (12*(extract (YEAR FROM age(visit.reported,from_date)))::int) + (extract (MONTH FROM age(visit.reported,from_date)))::int
      END AS interval_number,
      COUNT(DISTINCT visit.place_id)
    FROM
      VISITS_CTE visit
    LEFT JOIN
      {{ ref("contactview_metadata") }} cm ON cm.uuid = visit.place_id
    WHERE
      cm.type = 'clinic' AND
      NOT EXISTS (SELECT NULL FROM {{ ref("get_muted_contacts") }}(to_date,'clinic') muted WHERE muted.contact_uuid = visit.place_id)
    GROUP BY
      CHW_UUID,
      interval_number
  ) HH_VISITS ON (CHWLIST.CHW_UUID = HH_VISITS.CHW_UUID AND CHWLIST.interval_number = HH_VISITS.interval_number)

  LEFT JOIN

    (
      /*
        This query will provide the total number of community events conducted in the given period.
      */
      SELECT
        meta.reported_by AS CHW_UUID,
        CASE
          WHEN single_interval
          THEN 0
          ELSE (12*(extract (YEAR from age(reported,from_date)))::int) + (extract (MONTH from age(reported,from_date)))::int
        END AS interval_number,

        count(meta.uuid) AS COUNT

      FROM
        {{ ref("form_metadata") }} meta

      WHERE
        meta.form = 'community_event' AND
        meta.reported >= (date_trunc('day',from_date))::timestamp without time zone AND
        meta.reported < (date_trunc('day',to_date) + '1 day'::interval)::timestamp without time zone

      GROUP BY
        CHW_UUID,
        interval_number

    ) as COMMUNITYEVENT ON (CHWLIST.CHW_UUID = COMMUNITYEVENT.CHW_UUID AND CHWLIST.interval_number = COMMUNITYEVENT.interval_number)

  LEFT JOIN

    (

      WITH
        pnc_cte AS (
          SELECT DISTINCT(chw)
          FROM {{ ref("useview_postnatal_care") }} pnc
          WHERE
            pnc.follow_up_count = '1' AND
            pnc.reported >= (date_trunc('day',from_date))::timestamp without time zone AND
            pnc.reported < (date_trunc('day',to_date) + '1 day'::interval)::timestamp without time zone
        ),
        form_cte AS(
          SELECT DISTINCT(reported_by)
          FROM
            {{ ref("form_metadata") }} meta
          WHERE
            form IN ('pregnancy','assessment')
            AND meta.reported >= (date_trunc('day',from_date))::timestamp without time zone
            AND meta.reported < (date_trunc('day',to_date) + '1 day'::interval)::timestamp without time zone
        ),
        clinic_cte AS (
          SELECT DISTINCT(chw_uuid)
          FROM
            {{ ref("contactview_clinic") }} clinic
          WHERE
            clinic.created >= (date_trunc('day',from_date))::timestamp without time zone
            AND clinic.created < (date_trunc('day',to_date) + '1 day'::interval)::timestamp without time zone
        )

      SELECT
        chp.uuid AS CHW_UUID,
        0 as interval_number,
        (COALESCE(pnc.chw, meta.reported_by, clinic.chw_uuid) IS NOT NULL)::int AS active
      FROM
        {{ ref("contactview_chp") }} chp
      LEFT JOIN pnc_cte pnc ON pnc.chw = chp.uuid
      LEFT JOIN form_cte meta ON meta.reported_by = chp.uuid
      LEFT JOIN clinic_cte clinic ON clinic.chw_uuid = chp.uuid

    ) as ACTIVE_IN_RANGE ON (CHWLIST.CHW_UUID = ACTIVE_IN_RANGE.CHW_UUID AND CHWLIST.interval_number = ACTIVE_IN_RANGE.interval_number)
    LEFT JOIN (
    /*
      This query will provide the total number of community events conducted in the given period.
    */
    SELECT
      meta.reported_by AS CHW_UUID,
      CASE
        WHEN single_interval
        THEN 0
        ELSE (12*(extract (YEAR from age(reported,from_date)))::int) + (extract (MONTH from age(reported,from_date)))::int
      END AS interval_number,

      count(meta.uuid) AS COUNT

    FROM
      {{ ref("form_metadata") }} meta

    WHERE
      meta.form = 'family_survey' AND
      meta.reported >= (date_trunc('day',from_date))::timestamp without time zone AND
      meta.reported < (date_trunc('day',to_date) + '1 day'::interval)::timestamp without time zone

    GROUP BY
      CHW_UUID,
      interval_number
    ) AS FAMILY_SURVEY ON (CHWLIST.CHW_UUID = FAMILY_SURVEY.CHW_UUID AND CHWLIST.interval_number = FAMILY_SURVEY.interval_number)

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
