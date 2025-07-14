{% set COLUMNS = 'columns' %}
{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='sync_all_columns',
    indexes=[
      {COLUMNS: ['uuid'], 'type': 'hash'},
      {COLUMNS: ['user_name']},
      {COLUMNS: ['period_start']}
    ]
  )
}}
WITH filtered_metrics
AS (
  SELECT 
    telemetry.uuid,
    telemetry.saved_timestamp,
    telemetry.period_start,
    telemetry.user_name,
    REGEXP_REPLACE(KEY, '^.*?:.*?:(.*?):.*$', '\1') AS form_name,
    KEY,
    value
  FROM 
    {{ source('couchdb', env_var('POSTGRES_TABLE')) }} AS source_table
  CROSS JOIN jsonb_each(doc -> 'metrics') AS j(key,value)
  INNER JOIN {{ ref('telemetry') }} telemetry
    ON source_table._id = telemetry.uuid
  WHERE (
    KEY LIKE 'enketo:contacts:%'
    OR KEY LIKE 'enketo:tasks:%'
    OR KEY LIKE 'enketo:reports:%'
  )
  {% if is_incremental() %}
    AND telemetry.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
  {% endif %}
  )
SELECT 
  uuid,
  saved_timestamp,
  period_start,
  user_name,
  form_name,
  CASE 
    WHEN KEY LIKE 'enketo:reports:%' THEN 'reports'
    WHEN KEY LIKE 'enketo:tasks:%' THEN 'tasks'
    ELSE 'contacts'
    END AS source,
  SUM(CASE WHEN KEY LIKE 'enketo:%:render'THEN (value ->> 'sum')::BIGINT ELSE 0 END) AS form_render_time,
  SUM(CASE WHEN KEY LIKE 'enketo:%:render' THEN (value ->> 'count')::BIGINT ELSE 0 END) AS form_render_count,
  SUM(CASE WHEN KEY LIKE 'enketo:%:save' THEN (value ->> 'sum')::BIGINT ELSE 0 END) AS form_save_time,
  SUM(CASE WHEN KEY LIKE 'enketo:%:save'THEN (value ->> 'count')::BIGINT ELSE 0 END) AS form_save_count,
  SUM(CASE WHEN KEY LIKE 'enketo:%:user_edit_time' THEN (value ->> 'sum')::BIGINT ELSE 0 END) AS form_edit_time,
  SUM(CASE WHEN KEY LIKE 'enketo:%:user_edit_time' THEN (value ->> 'count')::BIGINT ELSE 0 END) AS form_edit_count
FROM 
  filtered_metrics
GROUP BY 
  uuid,
  saved_timestamp,
  period_start,
  user_name,
  form_name,
  source
