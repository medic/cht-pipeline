SELECT
FROM v1.couchdb couchdb
LEFT JOIN v1.data_record data_record ON couchdb._id = data_record.uuid
WHERE
  couchdb.doc->>'type' = 'data_record'
  -- TEST CONDITIONS
  AND (
    -- in couchdb, not in data_record
    (data_record.uuid IS NULL)
    OR -- fields dont match
    data_record.from_phone <> couchdb.doc->>'from' OR
    data_record.form <> couchdb.doc->>'form' OR
    data_record.patient_id <> couchdb.doc->>'patient_id' OR
    data_record.contact_uuid <> couchdb.doc->'contact'->>'_id'
  )
