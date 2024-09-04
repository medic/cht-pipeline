SELECT
FROM {{ source('couchdb', env_var('POSTGRES_TABLE')) }} couchdb
LEFT JOIN {{ ref('patient') }} patient ON couchdb._id = patient.uuid
WHERE
  couchdb._deleted = false
  AND
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
