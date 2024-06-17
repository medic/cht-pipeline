{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    indexes=[
      {'columns': ['"uuid"'], 'type': 'hash'},
      {'columns': ['"@timestamp"']},
      {'columns': ['place_id']},
    ]
  )
}}

SELECT
  uuid,
  contact."@timestamp",
  couchdb.doc->>'place_id' as place_id
FROM {{ ref('contact') }} contact
INNER JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb ON couchdb._id = uuid
WHERE 
  (couchdb.doc->>'place_id' IS NOT NULL) OR 
  (contact.contact_type <> 'person')
{% if is_incremental() %}
  AND contact."@timestamp" >= {{ max_existing_timestamp('"@timestamp"') }}
{% endif %}
