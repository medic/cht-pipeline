SELECT 
        doc ->> '_id'::text AS uuid,
        doc #>> '{fields,inputs,source}'::text[] AS source,
        doc #>> '{fields,inputs,source_id}'::text[] AS source_id,
        doc #>> '{contact,_id}'::text[] AS chw,
        doc #>> '{contact,_id}'::text[] AS reported_by,
        doc #>> '{contact,parent,_id}'::text[] AS reported_by_parent,
        doc #>> '{fields,patient_id}'::text[] AS patient_id,
        person.sex,
        doc ->> 'form'::text AS form,
        doc #>> '{fields,meta,instanceID}'::text[] AS instanceid,
        NULLIF(NULLIF(doc #>> '{fields,patient_age_in_days }', ''), 'NaN')::int AS patient_age_in_days,
        NULLIF(NULLIF(doc #>> '{fields,patient_age_in_months }', ''), 'NaN')::int AS patient_age_in_months,
        NULLIF(NULLIF(doc #>> '{fields,patient_age_in_years }', ''), 'NaN')::int AS patient_age_in_years,
        doc #>> '{fields,follow_up, status_updated}'::text[] AS hf_visit,
        doc #>> '{fields,follow_up, mnp}'::text[] AS fed_mnp,
        NULLIF(doc #>> '{fields,follow_up, satchet_count}'::text[], '')::int AS num_mnp_fed,
        doc #>> '{fields,follow_up, buy_mnp}'::text[] AS buy_mnp,
        NULLIF(doc #>> '{fields,follow_up, num_mnp}'::text[], '')::int AS num_mnp_bought,
        to_timestamp((NULLIF(doc ->> 'reported_date'::text, ''::text)::bigint / 1000)::double precision) AS reported
    FROM {{ ref("couchdb") }}
    LEFT JOIN
        {{ ref("contactview_person") }} person ON person.uuid = doc #>> '{fields,patient_id}'::text[]
    WHERE couchdb.doc ->> 'form'::text = 'muac_follow_up'