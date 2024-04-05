{{
    config(
        materialized = 'view',
    )
}}

SELECT
    doc->>'type' AS type,
    doc->>'reported_date' AS reported_date,
    *
FROM v1.{{ env_var('POSTGRES_TABLE') }}

{% if is_incremental() %}
    WHERE "reported_date" >= (SELECT MAX("reported_date") FROM {{ this }})
{% endif %}
