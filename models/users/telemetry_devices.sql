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
  telemetry.uuid,
  telemetry.saved_timestamp,
  telemetry.period_start,
  doc #>> '{device,deviceInfo,hardware,manufacturer}' AS device_manufacturer,
  doc #>> '{device,deviceInfo,hardware,model}' AS device_model,
  doc #>> '{dbInfo,doc_count}' AS doc_count,
  doc #>> '{device,userAgent}' AS user_agent,
  doc #>> '{device,deviceInfo,app,version}' AS cht_android_version,
  doc #>> '{device,deviceInfo,software,androidVersion}' AS android_version,
  doc #>> '{device,deviceInfo,storage,free}' AS storage_free,
  doc #>> '{device,deviceInfo,storage,total}' AS storage_total,
  doc #>> '{device,deviceInfo,network,upSpeed}' AS network_up_speed,
  doc #>> '{device,deviceInfo,network,downSpeed}' AS network_down_speed
FROM {{ ref('telemetry') }} telemetry
INNER JOIN
  {{ source('couchdb', env_var('POSTGRES_TABLE')) }} source_table
  ON source_table._id = telemetry.uuid
{% if is_incremental() %}
  WHERE telemetry.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
