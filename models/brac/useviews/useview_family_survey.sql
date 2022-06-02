{{
    config(
        materialized = 'incremental',
        unique_key='useview_family_survey_uuid',
        indexes=[
            {'columns': ['family_id']},
            {'columns': ['reported']},
            {'columns': ['"@timestamp"']}  
        ]
    )
}}

SELECT
{{ dbt_utils.surrogate_key(['uuid']) }} AS useview_family_survey_uuid,
*
FROM(
    SELECT
	    "@timestamp"::timestamp without time zone AS "@timestamp",
		doc->>'_id' AS uuid,
		doc#>>'{contact,_id}' AS chw,
		doc#>>'{contact,parent,_id}' AS area_uuid,
		to_timestamp((NULLIF(doc ->> 'reported_date', '')::bigint / 1000)::double precision) AS reported,
		date_trunc('day',to_timestamp((NULLIF(doc ->> 'reported_date', '')::bigint / 1000)::double precision))::date AS reported_day,
		doc#>>'{fields,place_id}' AS family_id,
		doc#>>'{fields,mosquito_nets}' AS mosquito_nets,
		doc#>>'{fields,hygeinic_toilet}' AS hygeinic_toilet,
		doc#>>'{fields,family_planning_method}' AS family_planning_method,
		doc#>>'{fields,source_of_drinking_water}' AS source_of_drinking_water,
		doc#>>'{fields,household_survey,g_handwashing_facility}' AS g_handwashing_facility,
		doc#>>'{fields,household_survey,g_improved_latrine}' AS g_improved_latrine,
		doc#>>'{fields,household_survey,g_open_defecation_free}' AS g_open_defecation_free
	FROM
		{{ ref("couchdb") }}

	WHERE
		 doc->>'form' = 'family_survey'
    {% if is_incremental() %}
            AND COALESCE("@timestamp" > (SELECT MAX({{ this }}."@timestamp") FROM {{ this }}), True)
    {% endif %}
) x