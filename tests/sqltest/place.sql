SELECT
FROM v1.couchdb couchdb
LEFT JOIN v1.place place ON couchdb._id = place.uuid
LEFT JOIN v1.contact contact ON contact.uuid = place.uuid
WHERE
  -- person conditions
  (
    (couchdb.doc->>'type' <> 'person') AND
    (couchdb.doc->>'type' = 'contact' AND couchdb.doc->>'contact_type' <> 'person')
  )
  -- TEST CONDITIONS
  AND (
    -- in couchdb, not in place
    place.uuid IS NULL OR
    -- a person, but not a contact?
    contact.uuid IS NULL
  )
