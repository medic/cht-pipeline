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
      {COLUMNS: ['user_name']},
      {COLUMNS: ['app_version']},
    ]
  )
}}
SELECT
  _id as  uuid,
  saved_timestamp,
  CONCAT_WS(
    '-',
    COALESCE(doc#>>'{metadata,year}','1970'),
    COALESCE(doc#>>'{metadata,month}','1'),
    COALESCE(doc#>>'{metadata,day}','1')
  )::timestamptz AS period_start,
  doc#>>'{metadata,user}' AS user_name,
  doc#>>'{metadata,versions,app}' AS app_version,
  doc#>>'{metrics,boot_time,min}' AS boot_time_min,
  doc#>>'{metrics,boot_time,max}' AS boot_time_max,
  doc#>>'{metrics,boot_time,count}' AS boot_time_count,
  doc#>>'{dbInfo,doc_count}' AS doc_count_on_local_db
FROM 
  {{ source('couchdb', env_var('POSTGRES_TABLE')) }} source_table
WHERE
  doc->>'type' = 'telemetry'
  AND _deleted = false
{% if is_incremental() %}
  AND source_table.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
