{{
  config(
    materialized = 'view',
    indexes=[
      {'columns': ['uuid']},
      {'columns': ['parent_uuid']},
      {'columns': ['type']},
      {'columns': ['uuid']},
    ]
  )
}}

WITH filtered_person AS (
  SELECT
    uuid,
    phone,
    phone2,
    date_of_birth,
    parent_uuid
  FROM {{ ref("contact") }}
  WHERE type = 'person'
)

SELECT
  person.uuid,
  person.phone,
  person.phone2,
  person.date_of_birth,
  parent.type AS parent_type
FROM filtered_person AS person
LEFT JOIN {{ ref("contact") }} AS parent
ON person.parent_uuid = parent.uuid;
