{{ config(materialized = 'materialized_view') }}

WITH settings AS (
  SELECT 
    jsonb_array_elements(source_table.doc->'settings'->'contact_types') as element
  FROM 
    {{ source('couchdb', env_var('POSTGRES_TABLE')) }} source_table
  WHERE _id = 'settings'
),
existing AS (
  SELECT
    DISTINCT contact_type as id
  FROM {{ ref('contact') }} 
)
SELECT
  COALESCE(settings.element->>'id', existing.id) as id,
  CASE
    WHEN id = 'person' THEN TRUE
    ELSE COALESCE(settings.element->>'person', 'false')::boolean
  END AS person,
  (settings.element IS NOT NULL) AS configured
FROM settings
FULL OUTER JOIN existing ON existing.id = settings.element->>'id'
