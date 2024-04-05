{{
  config(
    materialized = 'incremental',
  )
}}

SELECT
  contactview_hospital.uuid,
  contactview_hospital.name,
  contactview_hospital."@timestamp",
  couchdb.doc->>'area' AS area,
  couchdb.doc->>'region' AS region
FROM
  {{ ref("contactview_hospital") }}
  INNER JOIN {{ ref("couchdb") }} ON (couchdb.doc ->> '_id'::text = contactview_hospital.uuid AND couchdb.doc ->> 'type' = 'district_hospital')

{% if is_incremental() %}
  WHERE contactview_hospital."@timestamp" >= (SELECT MAX("@timestamp") FROM {{ this }})
{% endif %}
    