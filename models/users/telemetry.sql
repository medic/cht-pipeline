{{
  config(
    materialized = 'incremental',
    unique_key='telemetry_id',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['feedback_id'], 'type': 'hash'},
      {'columns': ['saved_timestamp']},
    ]
  )
}}

SELECT
  document_metadata.uuid as telemetry_id,
  document_metadata.saved_timestamp,
  CONCAT_WS(                                --> Date concatenation from JSON fields, eg. 2021-5-17
    '-',
    doc#>>'{metadata,year}',                --> year
    CASE                                    --> month of the year
      WHEN
        string_to_array(substring(doc#>>'{metadata,versions,app}' FROM '(\d+.\d+.\d+)'),'.')::int[] < '{3,8,0}'::int[]
      THEN
        (doc#>>'{metadata,month}')::int+1   --> Legacy, months zero-indexed (0 - 11)
      ELSE
        (doc#>>'{metadata,month}')::int     --> Month is between 1 - 12
    END,
    CASE                                    --> day of the month, else 1
      WHEN
        (doc#>>'{metadata,day}') IS NOT NULL
      THEN
        doc#>>'{metadata,day}'
      ELSE
        '1'
    END
  )::date AS period_start,
  doc#>>'{metadata,user}' AS user_name,
  doc#>>'{metadata,versions,app}' AS app_version,
  doc#>>'{metrics,boot_time,min}' AS boot_time_min,
  doc#>>'{metrics,boot_time,max}' AS boot_time_max,
  doc#>>'{metrics,boot_time,count}' AS boot_time_count,
  doc#>>'{dbInfo,doc_count}' AS doc_count_on_local_db
FROM {{ ref('document_metadata') }} document_metadata
INNER JOIN
  {{ source('couchdb', env_var('POSTGRES_TABLE')) }} source_table
  ON source_table._id = document_metadata.uuid
WHERE
  document_metadata.doc_type = 'telemetry'
  AND document_metadata._deleted = false
{% if is_incremental() %}
  AND document_metadata.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
