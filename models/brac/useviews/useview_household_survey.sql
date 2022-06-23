{{
    config(
        materialized = 'incremental',
        unique_key="uuid",
        indexes=[
            {'columns': ['chw']},
            {'columns': ['area_uuid']},
            {'columns': ['"@timestamp"']}           
        ]
    )
}}

SELECT
    doc ->> '_id'::text AS uuid,
    doc #>> '{parent,contact,_id}'::text[] AS chw,
    doc #>> '{parent,_id}'::text[] AS area_uuid,
    doc #>> '{household_survey,source_of_drinking_water}'::text[] AS source_of_drinking_water,
    doc #>> '{household_survey,hygeinic_toilet}'::text[] AS hygeinic_toilet,
    doc #>> '{household_survey,mosquito_nets}'::text[] AS mosquito_nets,
    to_timestamp((NULLIF(doc ->> 'reported_date'::text, ''::text)::bigint / 1000)::double precision) AS reported,
    date_trunc('day'::text, to_timestamp((NULLIF(doc ->> 'reported_date'::text, ''::text)::bigint / 1000)::double precision))::date AS reported_day 
FROM
    {{ ref("couchdb") }}
WHERE
    (doc ->> 'type') = 'clinic'
    {% if is_incremental() %}
        AND "@timestamp" > {{ max_existing_timestamp('"@timestamp"') }}
    {% endif %}