{{
  config(
    materialized = 'view',
  )
}}

SELECT
  contactview_hospital.uuid,
  contactview_hospital.name,
  couchdb.area,
  couchdb.region
FROM
  contactview_hospital
INNER JOIN couchdb
ON (couchdb.uuid = contactview_hospital.uuid AND couchdb.type = 'district_hospital');
