{{
    config(
        materialized = 'view',
        indexes=[
            {'columns': ['area_uuid']},
            {'columns': ['reported_month']},
            {'columns': ['"@timestamp"']}

        ]
    )
}}


SELECT
    "@timestamp"::timestamp without time zone AS "@timestamp",
    doc ->> '_id'::text AS xmlforms_uuid,
    doc #>> '{contact,_id}'::text[] AS reported_by,
    to_timestamp((NULLIF(doc ->> 'reported_date'::text, ''::text)::bigint / 1000)::double precision) AS reported,
    date_trunc('month'::text, to_timestamp((NULLIF(doc ->> 'reported_date'::text, ''::text)::bigint / 1000)::double precision)) AS reported_month,
    doc #>> '{contact,parent,_id}'::text[] AS area_uuid,
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
   FROM {{ ref("couchdb") }} form
  WHERE doc->>'type' = 'data_record' AND (doc ->> 'form'::text) = 'pregnancy_visit'::text
