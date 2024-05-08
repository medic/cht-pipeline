{{
  config(
    materialized = 'view',
  )
}}

SELECT
  cmd.uuid,
  cmd.name
FROM contactview_metadata AS cmd
WHERE cmd.type = 'district_hospital'::TEXT;
