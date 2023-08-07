{% set import_couchdb_data = select_table(source('couchdb','couchdb'), ref('couchdb_test_data')) %}

{{ print("import_couchdb_data: " ~ import_couchdb_data) }}

{{
    config(
        materialized = 'view',
        indexes=[
            {'columns': ['"@timestamp"'], 'type': 'brin'},
        ]
    )
}}

SELECT
    doc->>'type' AS type,
    *
FROM {{ import_couchdb_data }}
