{{
  config(
    materialized = 'view',
    indexes=[
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['name'], 'type': 'hash'},
      {'columns': ['"@timestamp"'], 'type': 'brin'},
      {'columns': ['_id', '_rev'], 'unique': True},
    ]
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
