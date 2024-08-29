SELECT
FROM {{ source('couchdb', env_var('POSTGRES_TABLE')) }} couchdb
LEFT JOIN {{ ref('data_record') }} data_record ON couchdb._id = data_record.uuid
WHERE
  couchdb.doc->>'type' = 'data_record'
  -- TEST CONDITIONS
  AND (
    -- in couchdb, not deleted, but not in data_record
    (data_record.uuid IS NULL AND couchdb._deleted = false)
    OR
    -- in couchdb, deleted, but in data_record
    (data_record.uuid IS NOT NULL AND couchdb._deleted = true)
    OR -- fields dont match
    data_record.from_phone <> couchdb.doc->>'from' OR
    data_record.form <> couchdb.doc->>'form' OR
    data_record.patient_id <> couchdb.doc->>'patient_id' OR
    data_record.contact_uuid <> couchdb.doc->'contact'->>'_id'
  )
