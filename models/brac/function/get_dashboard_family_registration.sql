{{ config(materialized = 'raw_sql') }}  


CREATE OR REPLACE FUNCTION {{ this }}(intervals INT DEFAULT NULL)
RETURNS TABLE(
  chp_id text,
  CHP_Name text,
  Branch_ID text,
  Branch_Name text,
  month text,
  hh_registrations int,
  supervisor_name text,
  visits int
)
AS
$BODY$

WITH CHPS_CTE AS (
  SELECT 
    uuid,
    name,
    area_uuid,
    branch_uuid,
    branch_name,
    supervisor_uuid
  FROM 
    {{ ref("contactview_chp") }} chp
  WHERE  
    EXISTS (SELECT NULL FROM {{ ref("contactview_metadata") }} cm WHERE chp.uuid = cm.contact_uuid AND "type" = 'health_center')   
),

REG_RECORDS_CTE AS (
  SELECT
    chp.uuid AS chp_id,
    (row_number() OVER (PARTITION BY chp.uuid ORDER BY to_char(date_interval,'YYYYMM') DESC))::int AS row_id,
    chp.name AS CHP_Name,
    chp.branch_uuid AS Branch_ID,
    chp.branch_name AS Branch_Name,
    to_char(date_interval,'YYYYMM') AS month,
    SUM(total)::int AS hh_registrations,
    supervisor.name AS supervisor_name,
    ABS(SUM(total))::int AS visits
  FROM 
    (
      (
        WITH muting AS (
          SELECT 
            parent_uuid AS area_uuid,
            date_trunc('month',date) AS date_interval,
            COUNT(contact_uuid) FILTER (WHERE mute_status IS TRUE) muted,
            COUNT(contact_uuid) FILTER (WHERE mute_status IS FALSE) unmuted
          FROM {{ ref("contactview_muted") }}
          WHERE "type" = 'clinic'
          GROUP BY 
            parent_uuid,date_interval
        )
        SELECT 
          area_uuid,
          date_interval,
          (unmuted - muted) AS total
        FROM muting
      )
      UNION ALL
      (
        SELECT 
          chw.area_uuid,
          date_trunc('month',family_contact.created) AS date_interval,
          COUNT(family_contact.uuid) AS total
        FROM {{ ref("contactview_family") }} family_contact
        LEFT JOIN CHPS_CTE chw 
        ON family_contact.chw_uuid = chw.uuid
        GROUP BY 
          area_uuid,
          date_interval
      )
  ) family
  LEFT JOIN 
    CHPS_CTE chp ON family.area_uuid = chp.area_uuid

  LEFT JOIN 
    {{ ref("contactview_metadata") }} supervisor ON chp.supervisor_uuid = supervisor.uuid 

  WHERE 
    chp.branch_name NOT IN('HQ', 'HQ OVC')

  GROUP BY 
    family.area_uuid,
    family.date_interval,
    chp.uuid,
    chp.name,
    chp.branch_uuid,
    chp.branch_name,
    supervisor.name
  ORDER BY
    chp.uuid,
    date_interval DESC
)

SELECT
  chp_id,
  CHP_Name,
  Branch_ID,
  Branch_Name,
  month,
  hh_registrations,
  supervisor_name,
  visits
FROM
  REG_RECORDS_CTE
WHERE intervals IS NULL OR row_id <= intervals

UNION ALL 

SELECT
  chp_id,
  CHP_Name,
  Branch_ID,
  Branch_Name,
  MAX(month) AS "month",
  COALESCE(SUM(hh_registrations), 0)::int AS hh_registrations,
  supervisor_name,
  COALESCE(SUM(visits) FILTER(WHERE row_id = (intervals + 1)), 0)::int AS visits
FROM
 REG_RECORDS_CTE
WHERE
 row_id > intervals
GROUP BY 
 chp_id,
 CHP_Name,
 Branch_ID,
 Branch_Name,
 supervisor_name

ORDER BY
 chp_id,
 month

$BODY$

LANGUAGE 'sql' STABLE;