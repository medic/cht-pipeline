{{
    config(
        materialized = 'incremental',
    )
}}

SELECT
    doc->>'type' AS type,
    *
FROM v1.{{ env_var('POSTGRES_TABLE') }}

{% if is_incremental() %}
    WHERE "@timestamp" >= (SELECT MAX("@timestamp") FROM {{ this }})
{% endif %}
