{{ config(materialized = 'raw_sql') }}  

CREATE MATERIALIZED VIEW contactview_chp AS
(
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
  JOIN {{ ref("contactview_branch") }} AS branch ON contactview_chw.branch_uuid = branch.uuid

);

CREATE INDEX contactview_chp_area_uuid ON public.contactview_chp USING btree (area_uuid);
CREATE INDEX contactview_chp_branch_uuid ON public.contactview_chp USING btree (branch_uuid);
CREATE INDEX contactview_chp_supervisor_uuid ON public.contactview_chp USING btree (supervisor_uuid);
CREATE UNIQUE INDEX contactview_chp_uuid ON public.contactview_chp USING btree (uuid);

ALTER  MATERIALIZED VIEW contactview_chp OWNER TO full_access;
GRANT SELECT ON contactview_chp TO klipfolio, brac_access;


  