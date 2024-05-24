{{
  config(
    materialized = 'view'
  )
}}

WITH combined_tables AS (
  SELECT * FROM {{ ref('new_couchdb') }}
  UNION ALL
  SELECT * FROM {{ ref('stable_couchdb') }}
)

SELECT * FROM combined_tables
