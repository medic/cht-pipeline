SELECT
FROM {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb
LEFT JOIN {{ ref('contact') }} contact ON couchdb._id = contact.uuid
WHERE
  couchdb.doc->>'type' IN ('contact', 'clinic', 'district_hospital', 'health_center', 'person')
  -- TEST CONDITIONS
  AND (
    (
      -- in couchdb, not in contact table
      (contact.uuid IS NULL)
      OR -- fields dont match
      contact.parent_uuid <> couchdb.doc->'parent'->>'_id' OR
      contact.contact_type <> COALESCE(couchdb.doc->>'contact_type', couchdb.doc->>'type') OR
      contact.phone <> couchdb.doc->>'phone'
    )
    OR 
    (
      -- contact has deleted flag
      couchdb.doc->>'_deleted' = 'true'
    )
  )
