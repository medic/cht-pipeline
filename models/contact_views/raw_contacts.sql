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
  recent_data.doc,
  recent_data."@timestamp"
FROM recent_data 
WHERE (recent_data.doc ->> 'type'::text) = ANY
  (ARRAY ['contact'::text, 'clinic'::text, 'district_hospital'::text, 'health_center'::text, 'person'::text])
