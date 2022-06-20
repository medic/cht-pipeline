SELECT
        doc ->> '_id'::text AS uuid,
        doc #>> '{fields,source}'::text[] AS source,
        doc #>> '{fields,source_id}'::text[] AS source_id,
        doc #>> '{contact,_id}' AS reported_by,
        doc #>> '{contact,parent,_id}' AS reported_by_parent,
        doc #>> '{fields,contact,_id}'::text[] AS patient_id,
        doc #>> '{fields,contact,name}'::text[] AS patient_name,
        NULLIF(NULLIF(doc #>> '{fields,patient_age_in_years }', ''), 'NaN')::int AS patient_age_in_years,
        doc #>> '{fields,date_of_death}'::text[] as date_of_death,
        (
            date_part('year', age(to_date(doc #>> '{fields,date_of_death}'::text[], 'YYYY-MM-DD'), to_date(person.date_of_birth,'YYYY-MM-DD'))) * 12 +
            date_part('month', age(to_date(doc #>> '{fields,date_of_death}'::text[], 'YYYY-MM-DD'), to_date(person.date_of_birth,'YYYY-MM-DD')))
        ) AS age_in_months,
        person.sex AS sex,
        to_timestamp((NULLIF(doc ->> 'reported_date', '')::bigint / 1000)::double precision) AS reported
    FROM {{ ref("couchdb") }} form
    LEFT JOIN {{ ref("contactview_person") }} person ON person.uuid = (doc ->> '_id'::text)
    WHERE doc->>'type' = 'data_record' AND doc ->> 'form' = 'death_confirmation'
