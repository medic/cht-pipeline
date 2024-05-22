{{
  config(
    materialized = 'materialized_view'
  )
}}

SELECT coalesce(max("@timestamp"), '1900-01-01') AS max_timestamp FROM {{ ref('data_record') }}