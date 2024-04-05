{{
  config(
    materialized = 'incremental',
  )
}}

SELECT
  cmd.uuid,
  cmd.name,
  cmd."@timestamp"
FROM {{ ref("contactview_metadata") }} cmd
WHERE cmd.type = 'district_hospital'::text

{% if is_incremental() %}
  AND "@timestamp" >= (SELECT MAX("@timestamp") FROM {{ this }})
{% endif %}
