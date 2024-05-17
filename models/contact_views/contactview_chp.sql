{{
  config(
    materialized = 'view',
  )
}}

SELECT
    chw.name,
    chw.uuid,
    user_settings.username,
    chw.phone,
    chw.phone2,
    chw.date_of_birth,
    chw.parent_type,
    chw.area_uuid,
    chw.branch_uuid,
    branch.name AS branch_name,
    branch.region,
    coalesce(nullif(chp.doc ->> 'supervisor'::TEXT, ''::TEXT), '563649afa0e2a13740a1982abc0a2d0d'::TEXT) AS supervisor_uuid,
    chp.doc #>> '{chp_profile, g_individual_info,nin}'::TEXT[] AS nin,
    chp.doc #>> '{chp_profile, g_individual_info,district_of_residence}'::TEXT[] AS district_of_residence,
    chp.doc #>> '{chp_profile, g_individual_info, county}'::TEXT[] AS county,
    chp.doc #>> '{chp_profile, g_individual_info, sub_county}'::TEXT[] AS sub_county,
    chp.doc #>> '{chp_profile, g_individual_info, parish}'::TEXT[] AS parish,
    chp.doc #>> '{chp_profile, g_individual_info, village}'::TEXT[] AS village,
    chp.doc #>> '{chp_profile, g_individual_info, dob}'::TEXT[] AS dob,
    chp.doc #>> '{chp_profile, g_individual_info, sex}'::TEXT[] AS sex,
    chp.doc #>> '{chp_profile, g_individual_info, marital_status}'::TEXT[] AS marital_status,
    chp.doc #>> '{chp_profile, g_contact_info, phone_number}'::TEXT[] AS phone_number,
    chp.doc #>> '{chp_profile, g_contact_info, alternate_number}'::TEXT[] AS alternate_number,
    chp.doc #>> '{chp_profile, g_contact_info, brac_bank}'::TEXT[] AS brac_bank_ac,
    chp.doc #>> '{chp_profile, g_position_info, chw_type}'::TEXT[] AS chw_type,
    chp.doc #>> '{chp_profile, g_position_info, start_date}'::TEXT[] AS enrolment_date,
    lower(chp.doc #>> '{chp_profile, g_position_info, facility_name}'::TEXT[]) AS facility_name,
    chp.doc #>> '{chp_profile, g_position_info, facility_level}'::TEXT[] AS facility_level,
    chp.doc #>> '{chp_profile, g_position_info, villages_served}'::TEXT[] AS villages_served,
    chp.doc #>> '{chp_profile, g_education, education_level}'::TEXT[] AS education_level,
    chp.doc #>> '{chp_profile, g_education, institution}'::TEXT[] AS institution,
    chp.doc #>> '{chp_profile, g_education, completion_year}'::TEXT[] AS completion_year,
    chp.doc #>> '{chp_profile, g_language, speak_english}'::TEXT[] AS speak_english,
    chp.doc #>> '{chp_profile, g_language, read_english}'::TEXT[] AS read_english,
    chp.doc #>> '{chp_profile, g_language, write_english}'::TEXT[] AS write_english,
    chp.doc #>> '{chp_profile, g_language, mother_tongue}'::TEXT[] AS mother_tongue,
    chp.doc #>> '{chp_profile, g_language, other_languages}'::TEXT[] AS other_languages,
    chp.doc #>> '{chp_profile, g_other_details, incentives}'::TEXT[] AS incentives,
    chp.doc #>> '{chp_profile, g_other_details, chp_services}'::TEXT[] AS chp_services
  FROM
    {{ ref("contactview_chw") }} chw
  INNER JOIN {{ ref("contact") }} AS cm ON chw.area_uuid = cm.uuid
  INNER JOIN {{ ref("contact") }} AS meta ON meta.uuid = chw.uuid
  INNER JOIN {{ ref("contactview_branch") }} AS branch ON chw.branch_uuid = branch.uuid
  LEFT JOIN {{ ref("contact") }} AS chp ON chp.uuid = chw.uuid
  LEFT JOIN 
    (
      SELECT
        contact_id,
        string_agg(doc ->>'name', ', ') AS username
      FROM {{ ref("couchdb") }} AS c
      WHERE type = 'user-settings' AND contact_id IS NOT NULL
      GROUP BY contact_id
    ) AS user_settings ON user_settings.contact_id = chw.uuid