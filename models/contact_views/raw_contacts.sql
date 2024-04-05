{{ 
  config(
    materialized='incremental',
  )
 }}

WITH max_timestamp AS (
  SELECT MAX(couchdb."@timestamp") AS max_timestamp
  FROM {{ ref("couchdb") }}
),
recent_data AS (
  SELECT *
  FROM {{ ref("couchdb") }} c
  {% if is_incremental() %}
    JOIN max_timestamp mt on c."@timestamp" >= mt.max_timestamp
  {% endif %}
)

SELECT
  recent_data.doc,
  recent_data."@timestamp"
FROM recent_data 
WHERE (recent_data.doc ->> 'type'::text) = ANY
  (ARRAY ['contact'::text, 'clinic'::text, 'district_hospital'::text, 'health_center'::text, 'person'::text])
