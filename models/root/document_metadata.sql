{% set import_couchdb_data = select_table("{{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }}", ref('test_source_table')) %}

{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    post_hook='delete from {{this}} where _deleted=true',
    indexes=[
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['saved_timestamp']},
      {'columns': ['doc_type']},
      {'columns': ['_deleted']},
    ]
  )
}}

SELECT
  _id as uuid,
  _deleted,
  saved_timestamp,
  doc->>'type' as doc_type
FROM {{ import_couchdb_data }}
{% if is_incremental() %}
WHERE source_table.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
