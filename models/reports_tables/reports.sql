{{
    config(
        materialized = 'incremental',
    )
}}

SELECT
    couchdb.doc ->> '_id'::text AS uuid,
    couchdb.doc ->> 'form'::text AS form,
    couchdb.doc -> 'geolocation' AS location,
    couchdb.doc ->> 'reported_date'::text AS reported_date,
    couchdb.doc #>> '{contact,_id}'::text[] AS contact_uuid
FROM {{ ref("couchdb") }}
WHERE (couchdb.doc ->> 'type'::text) = 'data_record'

{% if is_incremental() %}
    AND COALESCE("@timestamp" > (SELECT MAX("@timestamp") FROM {{ this }}), True)
{% endif %}
