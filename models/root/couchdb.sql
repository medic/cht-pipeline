{{
    config(
        materialized = 'raw_sql',
    )
}}

CREATE OR REPLACE VIEW {{ this }} AS
SELECT
    doc->>'type' AS type,
    *
FROM {{ env_var('ROOT_POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }}