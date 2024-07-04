SELECT
FROM v1.couchdb couchdb
LEFT JOIN v1.person person ON couchdb._id = person.uuid
LEFT JOIN v1.contact contact ON contact.uuid = person.uuid
WHERE
  -- person conditions
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
