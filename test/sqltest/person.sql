SELECT
FROM {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb
LEFT JOIN {{ ref('person') }} person ON couchdb._id = person.uuid
LEFT JOIN {{ ref('contact') }} contact ON contact.uuid = person.uuid
WHERE
  -- person conditions
  (
    (couchdb.doc->>'type' = 'person') OR
    (couchdb.doc->>'type' = 'contact' AND couchdb.doc->>'contact_type' = 'person')
  )
  -- TEST CONDITIONS
  AND (
    -- deleted is true
    (person.deleted = true)
  )
