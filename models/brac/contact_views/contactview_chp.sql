{{
    config(
        materialized = 'view',
        indexes=[
            {'columns': ['area_uuid']},
            {'columns': ['branch_uuid']},
            {'columns': ['supervisor_uuid']},
            {'columns': ['uuid']},
        ]
    )
}}

SELECT contactview_chw.name,
    contactview_chw.uuid,
    contactview_chw.phone,
    contactview_chw.phone2,
    contactview_chw.date_of_birth,
    contactview_chw.parent_type,
    contactview_chw.area_uuid,
    contactview_chw.branch_uuid,
    branch.name AS branch_name,
    branch.region,
    COALESCE(NULLIF(raw_contacts.doc ->> 'supervisor'::text, ''::text), '563649afa0e2a13740a1982abc0a2d0d'::text) AS supervisor_uuid
  FROM {{ ref("contactview_chw") }} AS contactview_chw  
  JOIN {{ ref("raw_contacts") }} AS raw_contacts ON contactview_chw.area_uuid = (raw_contacts.doc ->> '_id'::text)
  JOIN {{ ref("contactview_metadata") }} AS meta ON meta.uuid = contactview_chw.uuid
  JOIN {{ ref("contactview_branch") }} AS branch ON contactview_chw.branch_uuid = branch.uuid;


  