{{
    config(
        materialized = 'incremental',
        indexes=[
            {'columns': ['"@timestamp"'], 'type': 'brin'},
        ]
    )
}}

SELECT
    *
FROM {{ ref('couchdb') }}

{% if is_incremental() %}
  WHERE "@timestamp" > (SELECT max("@timestamp") FROM {{ this }})
{% endif %}
