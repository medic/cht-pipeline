{{
  config(
    materialized = 'view',
  )
}}

SELECT
  person.uuid,
  person.phone,
  person.phone2,
  person.date_of_birth,
  parent.type AS parent_type
FROM contactview_metadata AS person
LEFT JOIN contactview_metadata AS parent ON person.parent_uuid = parent.uuid
WHERE person.type = 'person'::TEXT;
