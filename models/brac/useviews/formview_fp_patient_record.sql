SELECT
        doc ->> '_id'::text AS uuid,
        doc #>> '{contact,_id}' AS reported_by,
        doc #>> '{contact,parent,_id}' AS reported_by_parent,
        doc #>> '{fields,inputs,source}'::text[] AS source,
        NULLIF(doc #>> '{fields,inputs,source_id}'::text[], '') AS source_id,
        doc ->> 'form' AS form,
        NULLIF(NULLIF(doc #>> '{fields,patient_age_in_years }', ''), 'NaN')::int AS patient_age_in_years,
        NULLIF(NULLIF(doc #>> '{fields,patient_age_in_months }', ''), 'NaN')::int AS patient_age_in_months,
        NULLIF(NULLIF(doc #>> '{fields,patient_age_in_days }', ''), 'NaN')::int AS patient_age_in_days,
        doc #>> '{fields,patient_id}'::text[] AS patient_id,
        doc #>> '{fields,patient_name}'::text[] AS patient_name,
        doc #>> '{fields,inputs,contact,sex}'::text[] AS sex,
        doc #>> '{fields,is_referral_case}'::text[] AS is_referral_case,
        COALESCE(doc #>> '{fields, group_fp, new_fp_method}'::text[], '') ~* 'referral' AS referred_for_fp_method,
        NULLIF(doc #>> '{fields, group_fp, risk_factors}'::text[], 'no_risk') IS NOT NULL AS referred_for_risks,
        doc #>> '{fields,group_fp,long_term_method}'::text[] AS longterm_fp_received,
        CASE
            WHEN doc ->> 'form' = 'fp_follow_up_refill'
            THEN doc #>> '{fields,refill_fp_given}'::text[]
            ELSE COALESCE(doc #>> '{fields,group_fp,new_fp_method}'::text[], doc #>> '{fields,group_fp,long_term_method}'::text[])
        END AS fp_given,
        (doc #>> '{fields,group_fp,new_fp_quantity}'::text[])::int AS new_fp_quantity,
        to_timestamp((NULLIF(doc ->> 'reported_date', '')::bigint / 1000)::double precision) AS reported
    FROM {{ ref("couchdb") }} AS form
    WHERE
        doc->>'type' = 'data_record' AND
    	(doc ->> 'form')::text IN ('family_planning', 'fp_follow_up_long_term', 'fp_follow_up_refill', 'fp_follow_up_prospective', 'fp_follow_up_short_term')