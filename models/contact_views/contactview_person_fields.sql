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

SELECT
  person.uuid,
  person.phone,
  person.phone2,
  person.date_of_birth,
  parent.type AS parent_type
FROM {{ ref("contact") }} AS person
LEFT JOIN {{ ref("contact") }} AS parent ON person.parent_uuid = parent.uuid
WHERE person.type = 'person'
