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
    *
FROM v1.{{ env_var('POSTGRES_TABLE') }}
{% if is_incremental() %}
    WHERE "@timestamp" >= (SELECT MAX("@timestamp") FROM {{ this }})
{% endif %}
