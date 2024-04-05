{{ 
  config(
    materialized='incremental',
  )
 }}

WITH recent_data AS (
  SELECT *
  FROM {{ ref("couchdb") }}
  {% if is_incremental() %}
    WHERE couchdb."@timestamp" >= (SELECT MAX(couchdb."@timestamp") FROM {{ this }})
  {% endif %}
)

SELECT
  couchdb.doc,
  couchdb."@timestamp"
FROM recent_data 
WHERE (couchdb.doc ->> 'type'::text) = ANY
  (ARRAY ['contact'::text, 'clinic'::text, 'district_hospital'::text, 'health_center'::text, 'person'::text])
