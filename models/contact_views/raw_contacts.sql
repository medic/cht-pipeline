{{
  config(
    materialized = 'view',
    indexes=[
      {'columns': ['type'], 'type': 'hash'},
      {'columns': ['"@timestamp"'], 'type': 'brin'},
      {'columns': ['_id', '_rev'], 'unique': True},
    ]
  )
}}

SELECT
  c.type,
  c._id,
  c._rev,
  c."@timestamp",
  c."@version",
  c.doc,
  c.doc_as_upsert
FROM {{ ref("couchdb") }} c
