{{ config(materialized = 'raw_sql') }} 

CREATE INDEX IF NOT EXISTS couchdb_doc_id ON {{ ref('couchdb') }} ((doc ->> '_id'::text) text_ops);