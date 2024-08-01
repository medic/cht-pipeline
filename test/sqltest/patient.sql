SELECT
FROM v1.couchdb couchdb
LEFT JOIN {{ ref('patient') }} patient ON couchdb._id = patient.uuid
WHERE
  (
    (couchdb.doc->>'type' = 'person') OR
    (couchdb.doc->>'type' = 'contact' AND couchdb.doc->>'contact_type' = 'person')
  ) AND
  couchdb.doc->>'patient_id' IS NOT NULL
  -- TEST CONDITIONS
  AND (
    -- deleted is true
    (patient.deleted = true)
  )
