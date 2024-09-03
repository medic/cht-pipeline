SELECT
FROM {{ source('couchdb', env_var('POSTGRES_TABLE')) }} couchdb
LEFT JOIN {{ ref('person') }} person ON couchdb._id = person.uuid
LEFT JOIN {{ ref('contact') }} contact ON contact.uuid = person.uuid
WHERE
  -- person conditions
  couchdb._deleted = false
  AND
  (
    (couchdb.doc->>'type' = 'person') OR
    (couchdb.doc->>'type' = 'contact' AND couchdb.doc->>'contact_type' = 'person')
  )
  -- TEST CONDITIONS
  AND (
    -- in couchdb, not in person
    person.uuid IS NULL OR
    -- a person, but not a contact?
    contact.uuid IS NULL
  )
