{{
    config(
        materialized = 'incremental',
        indexes=[
            {'columns': ['uuid']},
            {'columns': ['danger_sign_at_reg']},
            {'columns': ['reported']},
            {'columns': ['"@timestamp"']}

        ]
    )
}}

    WITH config_cte AS (
        SELECT ( value #>> '{lmp_calcs,maximum_days_pregnant}'::text[])::integer AS maximum_days_pregnant
        FROM {{ ref("configuration") }}
        WHERE key = 'anc'::text AND value ? 'lmp_calcs'::text
        )
    SELECT 
        preg."@timestamp"::timestamp without time zone AS "@timestamp",
        preg.uuid AS uuid,
        'pregnancy'::text AS form,
        preg.lmp <> '' AS has_lmp,
        preg.lmp::date,
        date_trunc('day', preg.reported::timestamp with time zone) <= (preg.lmp::date + '84 days'::interval) AS early_reg,
        (preg.lmp::date + '84 days'::interval)::date AS first_tri_end,
        (preg.lmp::date + '168 days'::interval)::date AS second_tri_end,
        preg.edd, 
        (preg.lmp::date + ((config.maximum_days_pregnant || ' days')::interval))::date AS mdd,
        date_part('days', now() - preg.lmp::date::timestamp with time zone)::integer AS days_since_lmp,
        date_part('days', preg.reported - preg.lmp::date::timestamp without time zone)::integer AS days_pregnant_at_reg,
        preg.danger_signs <> ''::text AS danger_sign_at_reg,
        preg.risk_factors <> ''::text AS has_risk_factor,
        preg.imported,
        preg.patient_id,
        preg.chw AS reported_by,
        contact.parent_uuid AS reported_by_parent,
        preg.reported
    FROM config_cte config,
        {{ ref("useview_pregnancy") }} preg
    INNER JOIN {{ ref("contactview_metadata") }} contact ON contact.uuid = preg.chw

    {% if is_incremental() %}
        AND preg . "@timestamp" > {{ max_existing_timestamp('"@timestamp"', target_ref=ref("useview_pregnancy")) }}
{% endif %}