SELECT
FROM v1.couchdb couchdb
LEFT JOIN v1.patient patient ON couchdb._id = patient.uuid
LEFT JOIN v1.contact contact ON couchdb._id = contact.uuid
WHERE
  (
    (couchdb.doc->>'type' = 'person') OR
    (couchdb.doc->>'type' = 'contact' AND couchdb.doc->>'contact_type' = 'person')
  ) AND
  couchdb.doc->>'patient_id' IS NOT NULL
  -- TEST CONDITIONS
  AND (
    -- in couchdb, not in patients
    patient.uuid IS NULL
  )
