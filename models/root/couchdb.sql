{{
    config(
        materialized = 'incremental',
        indexes=[
            {'columns': ['type'], 'type': 'hash'},
            {'columns': ['"@timestamp"'], 'type': 'brin'},
            {'columns': ['_id', '_rev'], 'unique': True},
        ]
    )
}}

SELECT
    doc->>'type' AS type,
    doc->>'_id' AS _id,
    doc->>'_rev' AS _rev,
    "@timestamp",
    "@version",
    doc,
    doc_as_upsert 
FROM v1.{{ env_var('POSTGRES_TABLE') }}
{% if is_incremental() %}
    WHERE "@timestamp" >= (SELECT MAX("@timestamp") FROM {{ this }})
{% endif %}
