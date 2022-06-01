SELECT
        form.doc ->> '_id'::text AS uuid,
        form.doc #>> '{fields,source}'::text[] AS source,
        form.doc #>> '{fields,source_id}'::text[] AS source_id,
        form.doc #>> '{contact,_id}' AS reported_by,
        form.doc #>> '{contact,parent,_id}' AS reported_by_parent,
        form.doc #>> '{fields,contact,_id}'::text[] AS patient_id,
        form.doc #>> '{fields,contact,name}'::text[] AS patient_name,
        NULLIF(NULLIF(form.doc #>> '{fields,patient_age_in_years }', ''), 'NaN')::int AS patient_age_in_years,
        form.doc #>> '{fields,date_of_death}'::text[] as date_of_death,
        (
            date_part('year', age(to_date(form.doc #>> '{fields,date_of_death}'::text[], 'YYYY-MM-DD'), to_date(person.date_of_birth,'YYYY-MM-DD'))) * 12 + 
            date_part('month', age(to_date(form.doc #>> '{fields,date_of_death}'::text[], 'YYYY-MM-DD'), to_date(person.date_of_birth,'YYYY-MM-DD')))
        ) AS age_in_months,
        person.sex AS sex,
        to_timestamp((NULLIF(form.doc ->> 'reported_date', '')::bigint / 1000)::double precision) AS reported
    FROM {{ ref("couchdb") }} form 
    JOIN {{ ref("form_metadata") }} fm ON fm.uuid = (form.doc ->> '_id'::text)
    LEFT JOIN {{ ref("contactview_person") }} person ON person.uuid = (form.doc ->> '_id'::text)
    WHERE form.doc ->> 'form' = 'death_confirmation'
