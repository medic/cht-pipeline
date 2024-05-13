{{
    config(materialized = 'view')
}}


SELECT
  uuid,

  COALESCE(couchdb.doc #>> '{fields,group_lmp,g_preg_res_kit}'::text[], couchdb.doc #>> '{fields,group_lmp,g_preg_res}'::text[], ''::text) AS preg_test,
  COALESCE(couchdb.doc #>> '{fields,inputs,meta,location,lat}',
      COALESCE(((regexp_split_to_array((couchdb.doc #>> '{fields,geolocation}'::text[]), ' '::text))[1]),'')
      ) AS latitude,

  COALESCE(couchdb.doc #>> '{fields,inputs,meta,location,long}',
      COALESCE(((regexp_split_to_array((couchdb.doc #>> '{fields,geolocation}'::text[]), ' '::text))[2]),'')
      ) AS longitude,

  COALESCE(couchdb.doc #>> '{fields,inputs,meta,location,error}','') AS geo_error,
  COALESCE(couchdb.doc #>> '{fields,inputs,meta,location,message}','') AS geo_message,

  ((couchdb.doc ->> 'content') = '<pregnancy version="old"><null/></pregnancy>') as imported,
  NULLIF(couchdb.doc #>> '{fields,lmp_date}','') as lmp,
  (couchdb.doc #>> '{fields,lmp_method}') as lmp_method,
  (couchdb.doc #>> '{fields,lmp_date_8601}') as lmp_date_8601,
  NULLIF(couchdb.doc #>> '{fields,edd}','')::date as edd,
  (couchdb.doc #>> '{fields,danger_signs}') AS danger_signs,
  (couchdb.doc #>> '{fields,risk_factors}') AS risk_factors,
  CASE
  WHEN couchdb.doc #>>'{fields,anc_visit_identifier}'::text[] <>''
  THEN (couchdb.doc #>>'{fields,anc_visit_identifier}')::int
  WHEN couchdb.doc #>>'{fields,group_repeat,anc_visit_repeat,anc_visit_identifier}'::text[] <>''
  THEN RIGHT(couchdb.doc #>>'{fields,group_repeat,anc_visit_repeat,anc_visit_identifier}'::text[],1) :: int
  ELSE 0
  END AS anc_visit

FROM
  {{ ref("data_record") }} couchdb
WHERE
  form = 'pregnancy'
