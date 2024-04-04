{{ 
      config(
            materialized='incremental',
      )
 }}

SELECT
      couchdb.doc,
      couchdb."@timestamp"
FROM {{ ref("couchdb") }}
WHERE (couchdb.doc ->> 'type'::text) = ANY
      (ARRAY ['contact'::text, 'clinic'::text, 'district_hospital'::text, 'health_center'::text, 'person'::text])

{% if is_incremental() %}
  WHERE couchdb."@timestamp" >= (SELECT MAX(couchdb."@timestamp") FROM {{ this }})
{% endif %}
