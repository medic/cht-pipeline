{{
    config(
        materialized = 'table',
        indexes=[
            {'columns': ['"@timestamp"'], 'type': 'brin'},
        ]
    )
}}

SELECT
    *
FROM {{ env_var('POSTGRES_TABLE') }}
