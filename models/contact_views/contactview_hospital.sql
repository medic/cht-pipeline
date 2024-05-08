{{
  config(
    materialized = 'view',
  )
}}

SELECT
  cmd.uuid,
  cmd.name
FROM {{ ref("contactview_metadata") }} AS cmd
WHERE cmd.type = 'district_hospital'
