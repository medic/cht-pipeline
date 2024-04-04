{{ 
      config(
            materialized='incremental',
      )
 }}

SELECT
      couchdb.doc
      couchdb."@timestamp"::timestamp without time zone AS "@timestamp",
FROM {{ ref("couchdb") }}
WHERE (couchdb.doc ->> 'type'::text) = ANY
      (ARRAY ['contact'::text, 'clinic'::text, 'district_hospital'::text, 'health_center'::text, 'person'::text])

{% if is_incremental() %}
  WHERE "@timestamp" >= (SELECT MAX("@timestamp") FROM {{ this }})
{% endif %}
