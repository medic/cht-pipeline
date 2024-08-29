SELECT
FROM {{ source('couchdb', env_var('POSTGRES_TABLE')) }} couchdb
LEFT JOIN {{ ref('user') }} cht_user ON couchdb._id = cht_user.user_id
WHERE
  couchdb.doc->>'type' = 'user-settings'
  -- TEST CONDITIONS
  AND (
    -- in couchdb, not deleted, but not in user table
    (cht_user.user_id IS NULL AND couchdb._deleted = false)
    OR
    -- in couchdb, deleted, but still in user table
    (cht_user.user_id IS NOT NULL AND couchdb._deleted = true)
  )
