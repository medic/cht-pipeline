{{
    config(
        materialized = 'incremental',
        indexes=[
            {'columns': ['"@timestamp"'], 'type': 'brin'},
        ]
    )
}}

SELECT
    *
FROM {{ env_var('POSTGRES_TABLE') }}

{% if is_incremental() %}
  WHERE "@timestamp" > (SELECT max("@timestamp") FROM {{ this }})
{% endif %}
