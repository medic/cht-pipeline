SELECT 
	couchdb.doc #>> '{contact,_id}'::text[] AS chw,
    couchdb.doc #>> '{contact,parent,_id}'::text[] AS area_uuid,
	couchdb.doc #>> '{fields,health_forum_general,health_forum_place}'::text[] AS health_forum_place,
    couchdb.doc #>> '{fields,health_forum_general,no_of_participants}'::text[] AS no_of_participants,
    couchdb.doc #>> '{fields,health_forum_general,conducted_by}'::text[] AS conducted_by,
    couchdb.doc #>> '{fields,health_forum_details,issue_discussed}'::text[] AS issue_discussed,
    couchdb.doc #>> '{fields,health_forum_details,decisions_taken}'::text[] AS decisions_taken,
    couchdb.doc #>> '{fields,health_forum_details,remarks}'::text[] AS remarks,
    couchdb.doc #>> '{fields,health_forum_sales,sales}'::text[] AS sales,
    couchdb.doc #>> '{fields,health_forum_sales,sales_cummulative}'::text[] AS sales_cummulative,
    to_timestamp((NULLIF(couchdb.doc ->> 'reported_date'::text, ''::text)::bigint / 1000)::double precision) AS reported, 
    date_trunc('day',to_timestamp((NULLIF(couchdb.doc ->> 'reported_date', '')::bigint / 1000)::double precision))::date AS reported_day,
	couchdb.doc ->> '_id'::text AS xmlforms_uuid
 FROM {{ ref("couchdb") }}
 WHERE 
(couchdb.doc ->> 'type'::text) = 'data_record'::text
AND (couchdb.doc ->> 'form'::text) = 'health_forum'::text