SELECT 
couchdb.doc, 
"@timestamp"::timestamp without time zone AS "@timestamp"
FROM {{ ref("couchdb") }}
WHERE (couchdb.doc ->> 'type'::text) = ANY
      (ARRAY ['contact'::text, 'clinic'::text, 'district_hospital'::text, 'health_center'::text, 'person'::text])
