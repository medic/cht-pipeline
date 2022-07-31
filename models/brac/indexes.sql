{{ config(schema='v1', materialized = 'raw_sql') }} 

CREATE INDEX IF NOT EXISTS couchdb_doc_id ON {{ ref('couchdb') }} ((doc ->> '_id'::text) text_ops);
CREATE INDEX IF NOT EXISTS couchdb_doc_type ON {{ ref('couchdb') }}((doc ->> 'type'::text) text_ops);
CREATE INDEX IF NOT EXISTS couchdb_doc_form ON {{ ref('couchdb') }}((doc ->> 'form'::text) text_ops) WHERE (doc ->> 'type'::text) = 'data_record'::text;
CREATE INDEX IF NOT EXISTS couchdb_doc_form_patient_id ON {{ ref('couchdb') }}((doc #>> '{fields,patient_id}'::text[]) text_ops) WHERE (doc ->> 'type'::text) = 'data_record'::text;
CREATE INDEX IF NOT EXISTS couchdb_doc_form_place_id ON {{ ref('couchdb') }}((doc #>> '{fields,place_id}'::text[]) text_ops) WHERE (doc ->> 'type'::text) = 'data_record'::text;
CREATE INDEX IF NOT EXISTS couchdb_doc_form_source_id ON {{ ref('couchdb') }}((doc #>> '{fields,inputs,source_id}'::text[]) text_ops) WHERE (doc ->> 'type'::text) = 'data_record'::text;
CREATE INDEX IF NOT EXISTS couchdb_doc_assessment_patient_age ON {{ ref('couchdb') }}((nullif(doc #>> '{fields,patient_age_in_years}', '')::int)) WHERE (doc ->> 'form'::text) = 'assessment'::text AND (doc #>> '{fields,patient_age_in_years}') != '';