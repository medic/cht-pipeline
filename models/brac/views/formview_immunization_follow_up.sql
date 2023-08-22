SELECT
        form.doc ->> '_id'::text AS uuid,
        COALESCE(form.doc #>> '{fields,inputs,contact,_id}'::text[], patient.doc ->> '_id'::text) AS patient_id,
        COALESCE(form.doc #>> '{fields,inputs,contact,name}'::text[], patient.doc ->> 'name'::text) AS patient_name,
        (form.doc #>> '{fields,patient_age_in_years}')::int AS patient_age_in_years,
        (form.doc #>> '{fields,patient_age_in_months}')::int AS patient_age_in_months,
        form.doc #>> '{fields,inputs,source}'::text[] AS "inputs/source",
        form.doc #>> '{fields,inputs,source_id}'::text[] AS "inputs/source_id",
        form.doc #>> '{contact,_id}' AS reported_by,
        form.doc #>> '{contact,parent,_id}' AS reported_by_parent,
        form.doc #>> '{fields,follow_up,vaccines_received}'::text[] AS vaccines_received,
        form.doc #>> '{fields,follow_up,status_updated}'::text[] AS status_updated,
        form.doc #>> '{fields,follow_up,vaccines_administered}'::text[] AS vaccines_administered,
        to_timestamp((NULLIF(form.doc ->> 'reported_date', '')::bigint / 1000)::double precision) AS reported
    FROM {{ ref("couchdb") }} form
    JOIN {{ ref("form_metadata") }} fm ON fm.uuid = (form.doc ->> '_id'::text)
    LEFT JOIN {{ ref("couchdb") }} patient ON (patient.doc ->> '_id'::text) = (form.doc #>> '{fields,patient_id}'::text[])
    WHERE (form.doc ->> 'form'::text) = 'immunization_follow_up'::text