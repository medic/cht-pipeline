{{
    config(
        materialized = 'incremental',
        unique_key="uuid",
        indexes=[
            {'columns': ['xmlforms_uuid']}         
        ]
    )
}}
 
SELECT 
    "@timestamp"::timestamp without time zone AS "@timestamp",
    doc ->> '_id'::text AS xmlforms_uuid,
	doc #>> '{contact,_id}'::text[] AS chw,
    doc #>> '{contact,parent,_id}'::text[] AS area_uuid,
	doc #>> '{fields,health_forum_general,health_forum_place}'::text[] AS health_forum_place,
    doc #>> '{fields,health_forum_general,no_of_participants}'::text[] AS no_of_participants,
    doc #>> '{fields,health_forum_general,conducted_by}'::text[] AS conducted_by,
    doc #>> '{fields,health_forum_details,issue_discussed}'::text[] AS issue_discussed,
    doc #>> '{fields,health_forum_details,decisions_taken}'::text[] AS decisions_taken,
    doc #>> '{fields,health_forum_details,remarks}'::text[] AS remarks,
    doc #>> '{fields,health_forum_sales,sales}'::text[] AS sales,
    doc #>> '{fields,health_forum_sales,sales_cummulative}'::text[] AS sales_cummulative,
    to_timestamp((NULLIF(doc ->> 'reported_date'::text, ''::text)::bigint / 1000)::double precision) AS reported, 
    date_trunc('day',to_timestamp((NULLIF(doc ->> 'reported_date', '')::bigint / 1000)::double precision))::date AS reported_day
 FROM {{ ref("couchdb") }}
 WHERE 
(doc ->> 'type'::text) = 'data_record'::text
AND (doc ->> 'form'::text) = 'health_forum'::text
{% if is_incremental() %}
    AND "@timestamp" > {{ max_existing_timestamp('"@timestamp"') }}
{% endif %}