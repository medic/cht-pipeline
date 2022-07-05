{{
    config(
        materialized = 'view',
        indexes=[
            {'columns': ['"@timestamp"'], 'type': 'btree'},
        ]
    )
}}

SELECT
    doc->>'type' AS type,
    *
FROM {{ env_var('ROOT_POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }}