{{
    config(
        materialized = 'view',
        indexes=[
            {'columns': ['"@timestamp"'], 'type': 'brin'},
        ]
    )
}}

SELECT
    doc->>'type' AS type,
    *
FROM {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }}
