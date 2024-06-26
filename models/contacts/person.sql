{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns'
  )
}}

SELECT
  contact.uuid,
  contact."@timestamp",
  couchdb.doc->>'date_of_birth' as date_of_birth,
  couchdb.doc->>'sex' as sex
FROM {{ ref("contact") }} contact
INNER JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb ON couchdb._id = uuid
WHERE contact.contact_type = 'person'
{% if is_incremental() %}
  AND contact."@timestamp" >= {{ max_existing_timestamp('"@timestamp"') }}
{% endif %}
