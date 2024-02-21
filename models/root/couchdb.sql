{% set import_couchdb_data = select_table(source('medic','medic'), ref('medic_test_data')) %}

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
