{{
    config( materialized = 'view' )
}}


SELECT
  uuid,

  doc#>>'{fields,place_id}' AS family_id,
  doc#>>'{fields,mosquito_nets}' AS mosquito_nets,
  doc#>>'{fields,hygeinic_toilet}' AS hygeinic_toilet,
  doc#>>'{fields,family_planning_method}' AS family_planning_method,
  doc#>>'{fields,source_of_drinking_water}' AS source_of_drinking_water,
  doc#>>'{fields,household_survey,g_handwashing_facility}' AS g_handwashing_facility,
  doc#>>'{fields,household_survey,g_improved_latrine}' AS g_improved_latrine,
  doc#>>'{fields,household_survey,g_open_defecation_free}' AS g_open_defecation_free
FROM
  {{ ref("data_record") }}
WHERE
   form = 'family_survey'
