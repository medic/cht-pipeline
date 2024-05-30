{{
  config(
    materialized = 'view',
  )
}}

SELECT
  cmd.uuid,
  cmd.name
FROM {{ ref("contact") }} AS cmd
WHERE cmd.type = 'district_hospital'
