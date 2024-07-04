{{
  config(
    materialized = 'incremental',
    unique_key='user_id',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['user_id'], 'type': 'hash'},
      {'columns': ['saved_timestamp']},
    ]
  )
}}

SELECT
  _id as user_id,
  saved_timestamp,
  COALESCE(
    doc->>'contact_id',
    doc->>'facility_id'
  ) as contact_uuid,
  doc->>'language' as language,
  doc->>'roles' as roles
FROM {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }}
WHERE doc->>'type' = 'user-settings'
{% if is_incremental() %}
  AND saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
