SELECT COUNT(*)
FROM {{ ref('document_metadata') }}
WHERE _deleted = true
