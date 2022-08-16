{{
    config(
        materialized = 'incremental',
        indexes=[
            {'columns': ['area_uuid']},
            {'columns': ['branch_uuid']},
            {'columns': ['supervisor_uuid']}
        ]
    )
}}

SELECT
    contactview_chw.name,
    contactview_chw.uuid,
    contactview_chw.phone,
    contactview_chw.phone2,
    contactview_chw.date_of_birth,
    contactview_chw.parent_type,
    contactview_chw.area_uuid,
    contactview_chw.branch_uuid,
    branch.name AS branch_name,
    branch.region,
    COALESCE(NULLIF(raw_contacts.doc ->> 'supervisor'::text, ''::text), '563649afa0e2a13740a1982abc0a2d0d'::text) AS supervisor_uuid,
    chp.doc #>> '{chp_profile, g_individual_info,nin}'::text[] AS nin,
    chp.doc #>> '{chp_profile, g_individual_info,district_of_residence}'::text[] AS district_of_residence,
    chp.doc #>> '{chp_profile, g_individual_info, county}'::text[] AS county,
    chp.doc #>> '{chp_profile, g_individual_info, sub_county}'::text[] AS sub_county,
    chp.doc #>> '{chp_profile, g_individual_info, parish}'::text[] AS parish,
    chp.doc #>> '{chp_profile, g_individual_info, village}'::text[] AS village,
    chp.doc #>> '{chp_profile, g_individual_info, dob}'::text[] AS dob,
    chp.doc #>> '{chp_profile, g_individual_info, sex}'::text[] AS sex,
    chp.doc #>> '{chp_profile, g_individual_info, marital_status}'::text[] AS marital_status,
    chp.doc #>> '{chp_profile, g_contact_info, phone_number}'::text[] AS phone_number,
    chp.doc #>> '{chp_profile, g_contact_info, alternate_number}'::text[] AS alternate_number,
    chp.doc #>> '{chp_profile, g_contact_info, brac_bank}'::text[] AS brac_bank_ac,
    chp.doc #>> '{chp_profile, g_position_info, chw_type}'::text[] AS chw_type,
    chp.doc #>> '{chp_profile, g_position_info, start_date}'::text[] AS enrolment_date,
    LOWER(chp.doc #>> '{chp_profile, g_position_info, facility_name}'::text[]) AS facility_name,
    chp.doc #>> '{chp_profile, g_position_info, facility_level}'::text[] AS facility_level,
    chp.doc #>> '{chp_profile, g_position_info, villages_served}'::text[] AS villages_served,
    chp.doc #>> '{chp_profile, g_education, education_level}'::text[] AS education_level,
    chp.doc #>> '{chp_profile, g_education, institution}'::text[] AS institution,
    chp.doc #>> '{chp_profile, g_education, completion_year}'::text[] AS completion_year,
    chp.doc #>> '{chp_profile, g_language, speak_english}'::text[] AS speak_english,
    chp.doc #>> '{chp_profile, g_language, read_english}'::text[] AS read_english,
    chp.doc #>> '{chp_profile, g_language, write_english}'::text[] AS write_english,
    chp.doc #>> '{chp_profile, g_language, mother_tongue}'::text[] AS mother_tongue,
    chp.doc #>> '{chp_profile, g_language, other_languages}'::text[] AS other_languages,
    chp.doc #>> '{chp_profile, g_other_details, incentives}'::text[] AS incentives,
    chp.doc #>> '{chp_profile, g_other_details, chp_services}'::text[] AS chp_services,
    raw_contacts."@timestamp"::timestamp without time zone AS "@timestamp"
  FROM
    {{ ref("contactview_chw") }}
  JOIN {{ ref("raw_contacts") }} ON contactview_chw.area_uuid = (raw_contacts.doc ->> '_id'::text)
  JOIN {{ ref("contactview_branch") }} branch ON contactview_chw.branch_uuid = branch.uuid
  JOIN {{ ref("raw_contacts") }} chp ON (chp.doc ->> '_id'::text) = contactview_chw.uuid

    {% if is_incremental() %}
        WHERE raw_contacts."@timestamp" > {{ max_existing_timestamp('"@timestamp"', target_ref=ref("raw_contacts")) }}
    {% endif %}