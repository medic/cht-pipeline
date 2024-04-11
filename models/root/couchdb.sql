{{
    config(
        materialized = 'view',
        unique_key = ['_id', '_rev'],
        indexes=[
            {'columns': ['type'], 'type': 'hash'},
            {'columns': ['"@timestamp"'], 'type': 'brin'},
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
