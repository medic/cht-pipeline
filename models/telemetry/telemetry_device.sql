{% set COLUMNS = 'columns' %}
{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    indexes=[
      {COLUMNS: ['uuid'], 'type': 'hash'},
      {COLUMNS: ['saved_timestamp']},
      {COLUMNS: ['period_start']},
      {COLUMNS: ['android_version']},
    ]
  )
}}

SELECT
  _id as uuid,
  saved_timestamp,
  CONCAT_WS(
    '-',
    COALESCE(doc#>>'{metadata,year}','1970'),
    COALESCE(doc#>>'{metadata,month}','1'),
    COALESCE(doc#>>'{metadata,day}','1')
  )::timestamptz AS period_start,
	doc #>>'{metadata,user}' AS user_name,
  doc #>> '{metadata,deviceId}' as device_id,
  doc #>> '{device,deviceInfo,hardware,manufacturer}' AS device_manufacturer,
  doc #>> '{device,deviceInfo,hardware,model}' AS device_model,
  doc #>> '{device,userAgent}' AS user_agent,
  doc #>> '{device,deviceInfo,app,version}' AS cht_android_version,
  doc #>> '{device,deviceInfo,software,androidVersion}' AS android_version,
  doc #>> '{device,deviceInfo,storage,free}' AS storage_free,
  doc #>> '{device,deviceInfo,storage,total}' AS storage_total,
  doc #>> '{device,deviceInfo,network,upSpeed}' AS network_up_speed,
  doc #>> '{device,deviceInfo,network,downSpeed}' AS network_down_speed
FROM 
  {{ source('couchdb', env_var('POSTGRES_TABLE')) }} source_table
WHERE
  doc->>'type' = 'telemetry'
  AND _deleted = false
{% if is_incremental() %}
  AND source_table.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
