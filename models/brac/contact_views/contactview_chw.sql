SELECT
    raw_contacts.doc ->> '_id'::text AS uuid,
    raw_contacts.doc ->> 'name'::text AS name,
    raw_contacts.doc ->> 'type'::text AS type,
    raw_contacts.doc ->> 'contact_type'::text    AS contact_type,
    raw_contacts.doc #>> '{contact,_id}'::text[] AS contact_uuid,
    raw_contacts.doc #>> '{parent,_id}'::text[] AS parent_uuid,
    raw_contacts.doc ->> 'notes'::text AS notes,    
    '1970-01-01 03:00:00+03'::timestamp with time zone +
    (((raw_contacts.doc ->> 'reported_date'::text)::numeric) / 1000::numeric)::double precision *
    '00:00:01'::interval AS reported
FROM {{ ref("contactview_person_fields") }} AS pplfields  
  JOIN {{ ref("contactview_metadata") }} AS chw ON contactview_chw.area_uuid = (raw_contacts.doc ->> '_id'::text)
  JOIN {{ ref("contactview_metadata") }} AS meta ON meta.uuid = contactview_chw.uuid
  WHERE pplfields.parent_type = 'health_center'::text