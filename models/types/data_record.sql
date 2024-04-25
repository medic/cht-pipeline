{{
    config(
        materialized = 'incremental',
        indexes=[
            {'columns': ['"@timestamp"'], 'type': 'brin'},
            {'columns': ['"form"'], 'type': 'hash'},
        ]
    )
}}

SELECT
    doc->>'reported_date' AS reported_date,
    doc->>'patient_id' AS patient_id,
    type,
    form,
    _id,
    _rev,
    doc,
    "@timestamp",
    "@version"
FROM {{ ref('couchdb') }}
WHERE
    type = 'data_record'
{% if is_incremental() %}
    AND COALESCE("@timestamp" > (SELECT MAX("@timestamp") FROM {{ this }}), True)
{% endif %}
