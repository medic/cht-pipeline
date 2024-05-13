{{
    config(materialized = 'view')
}}


SELECT
  uuid,

  doc #>> '{fields,inputs,source}'::text[] AS "inputs/source",
  doc #>> '{fields,inputs,source_id}'::text[] AS "inputs/source_id",
  doc #>> '{fields,inputs,t_lmp_date}'::text[] AS "inputs/t_lmp_date",
  doc #>> '{fields,lmp_date}'::text[] AS lmp_date,
  doc #>> '{fields,follow_up_method}'::text[] AS follow_up_method,
  doc #>> '{fields,danger_signs}'::text[] AS danger_signs,
  doc #>> '{fields,days_since_lmp}'::text[] AS days_since_lmp,
  doc #>> '{fields,weeks_since_lmp}'::text[] AS weeks_since_lmp,
  doc #>> '{fields,edd}'::text[] AS edd,
  COALESCE(doc #>> '{fields,group_tt,mother_tt}'::text[], doc #>> '{fields,g_tt,mother_tt_rcvd}'::text[], '') AS tt,
  COALESCE(doc #>> '{fields,group_repeat,anc_visit_repeat,anc_visit_identifier}'::text[],
  doc #>> '{fields,g_anc_visit,anc_visit_type}'::text[], '') AS anc_visit
FROM
  {{ ref("data_record") }}
WHERE
  form = 'pregnancy_visit'
