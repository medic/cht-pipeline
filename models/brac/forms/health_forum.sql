{{
    config(materialized = 'view')
}}

SELECT
  uuid,

	doc #>> '{fields,health_forum_general,health_forum_place}'::text[] AS health_forum_place,
  doc #>> '{fields,health_forum_general,no_of_participants}'::text[] AS no_of_participants,
  doc #>> '{fields,health_forum_general,conducted_by}'::text[] AS conducted_by,
  doc #>> '{fields,health_forum_details,issue_discussed}'::text[] AS issue_discussed,
  doc #>> '{fields,health_forum_details,decisions_taken}'::text[] AS decisions_taken,
  doc #>> '{fields,health_forum_details,remarks}'::text[] AS remarks,
  doc #>> '{fields,health_forum_sales,sales}'::text[] AS sales,
  doc #>> '{fields,health_forum_sales,sales_cummulative}'::text[] AS sales_cummulative
FROM
  {{ ref("data_record") }}
WHERE
  form = 'health_forum'
