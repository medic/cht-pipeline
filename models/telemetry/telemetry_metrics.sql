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
    sum(CASE when key like 'tasks:load' THEN (value->>'sum')::bigint ELSE 0 END) AS total_tasks_page_load_time,
    sum(CASE when key like 'tasks:load' THEN (value->>'count')::bigint ELSE 0 END) AS total_tasks_page_load_count,
    sum(CASE when key like 'tasks:refresh' THEN (value->>'sum')::bigint ELSE 0 END) AS total_tasks_page_refresh_time,
    sum(CASE when key like 'tasks:refresh' THEN (value->>'count')::bigint ELSE 0 END) AS total_tasks_page_refresh_count,
    sum(CASE WHEN KEY LIKE 'contact_detail:%:load' THEN (value->>'sum')::bigint ELSE 0 END) AS total_contact_load_time,
    sum(CASE WHEN KEY LIKE 'contact_detail:%:load' THEN (value->>'count')::bigint ELSE 0 END) AS total_contact_load_count,
    sum(CASE WHEN KEY LIKE 'contact_detail:%:load_tasks' THEN (value->>'sum')::bigint ELSE 0 END) AS total_contact_tasks_load_time,
    sum(CASE WHEN KEY LIKE 'contact_detail:%:load_tasks' THEN (value->>'count')::bigint ELSE 0 END) AS total_contact_tasks_load_count,
    sum(CASE WHEN KEY LIKE 'replication:medic:%:success' THEN (value->>'sum')::bigint ELSE 0 END) AS total_replication_time,
    sum(CASE WHEN KEY LIKE 'replication:medic:%:success' THEN (value->>'count')::bigint ELSE 0 END) AS total_replication_count,
    sum(CASE WHEN KEY LIKE 'replication:medic:%:failure:reason:offline:client' THEN (value->>'count')::bigint ELSE 0 END) AS replication_fail_client_count,
    sum(CASE WHEN KEY LIKE 'replication:user-initiated' THEN (value ->>'count')::int ELSE 0 END) AS count_user_initiated_replication,
    sum(CASE WHEN KEY LIKE '%:apdex:%' THEN (value ->>'count')::int ELSE 0 END) AS count_any_interaction,
    sum(CASE WHEN KEY LIKE '%:apdex:%' AND KEY LIKE '%analytics:targets%' THEN (value ->>'count')::int ELSE 0 END) AS count_analytics_interaction,
    sum(CASE WHEN KEY LIKE '%:apdex:%' AND KEY LIKE '%:add:save%' THEN (value ->>'count')::int ELSE 0 END) AS count_report_interaction,
    sum(CASE WHEN KEY LIKE '%:apdex:%' AND KEY LIKE '%enketo:report:%:save%' THEN (value ->>'count')::int ELSE 0 END) AS count_submission_by_report_tab,
    sum(CASE WHEN KEY LIKE '%:apdex:%' AND KEY LIKE '%enketo:tasks:%:save%' THEN (value ->>'count')::int ELSE 0 END) AS count_submission_by_task_tab
FROM
  {{ source('couchdb', env_var('POSTGRES_TABLE')) }} source_table
  CROSS JOIN LATERAL jsonb_each(doc->'metrics') AS j(key, value)
WHERE
  doc->>'type' = 'telemetry'
  AND _deleted = false
{% if is_incremental() %}
  AND source_table.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
GROUP BY
  1,2,3,4
