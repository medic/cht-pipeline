{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    indexes=[
      {'columns': ['"@timestamp"'], 'type': 'btree'},
      {'columns': ['reported'], 'type': 'brin'},
      {'columns': ['uuid']},
    ]
  )
}}

SELECT
  uuid,
  name,
  date_of_birth,
  doc ->> 'reported_date'::text AS reported,
  "@timestamp"

FROM {{ ref('couchdb') }}
WHERE type = 'person'
{% if is_incremental() %}
  AND "@timestamp" >= {{ max_existing_timestamp('"@timestamp"') }}
{% endif %}
