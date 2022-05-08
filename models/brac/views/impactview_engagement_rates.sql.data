SELECT branch.name AS "Branch Name",
    count(DISTINCT
        CASE
            WHEN (date_trunc('month'::text, fm.reported) = date_trunc('month'::text, now())) THEN fm.chw
            ELSE NULL::text
        END) AS "MTD",
    count(DISTINCT
        CASE
            WHEN (date_trunc('month'::text, fm.reported) = date_trunc('month'::text, (now() - '1 mon'::interval))) THEN fm.chw
            ELSE NULL::text
        END) AS "Last month",
    count(DISTINCT
        CASE
            WHEN (date_trunc('month'::text, fm.reported) = date_trunc('month'::text, (now() - '2 mons'::interval))) THEN fm.chw
            ELSE NULL::text
        END) AS "2 months ago",
    count(DISTINCT
        CASE
            WHEN (date_trunc('month'::text, fm.reported) = date_trunc('month'::text, (now() - '3 mons'::interval))) THEN fm.chw
            ELSE NULL::text
        END) AS "3 months ago",
    count(DISTINCT
        CASE
            WHEN (date_trunc('month'::text, fm.reported) = date_trunc('month'::text, (now() - '4 mons'::interval))) THEN fm.chw
            ELSE NULL::text
        END) AS "4 months ago",
    count(DISTINCT
        CASE
            WHEN (date_trunc('month'::text, fm.reported) = date_trunc('month'::text, (now() - '5 mons'::interval))) THEN fm.chw
            ELSE NULL::text
        END) AS "5 months ago",
    count(DISTINCT
        CASE
            WHEN (date_trunc('month'::text, fm.reported) = date_trunc('month'::text, (now() - '6 mons'::interval))) THEN fm.chw
            ELSE NULL::text
        END) AS "6 months ago",
    count(DISTINCT
        CASE
            WHEN ((age(date_trunc('month'::text, now()), (date_trunc('month'::text, fm.reported))::timestamp with time zone) >= '1 mon'::interval) AND (age(date_trunc('month'::text, now()), (date_trunc('month'::text, fm.reported))::timestamp with time zone) <= '3 mons'::interval)) THEN fm.chw
            ELSE NULL::text
        END) AS "Any time in last 3 months",
    count(DISTINCT fm.chw) AS "HWs submitting a valid form all time"
FROM ((({{ ref("form_metadata") }} fm
    JOIN {{ ref("contactview_chp") }} chp ON ((fm.chw = chp.uuid)))
    JOIN {{ ref("contactview_branch") }} branch ON ((chp.branch_uuid = branch.uuid)))
    JOIN {{ ref("couchdb") }} ON ((((couchdb.doc ->> '_id'::text) = fm.uuid) AND ((couchdb.doc ->> 'content'::text) <> '<pregnancy version="old"><null/></pregnancy>'::text))))
GROUP BY branch.name