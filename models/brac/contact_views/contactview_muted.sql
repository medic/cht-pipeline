    SELECT 
        mute_log.contact_uuid,
        contact.type,
        contact.parent_uuid,
        mute_log.mute_status,
        mute_log.date
    FROM (
        SELECT 
            contact_uuid,
            "date"::timestamp with time zone,
            muted as mute_status,
            report_id
        FROM(
                SELECT 
                    doc ->> 'doc_id' AS contact_uuid,
                    doc ->> 'muting_history' AS muting_history 
                FROM
                {{ ref("couchdb") }} 
                WHERE 
                    doc ->> 'type'::text = 'info' AND doc->>'muting_history' IS NOT NULL
        
            ) AS muting
            CROSS JOIN LATERAL json_populate_recordset(null::record, muting.muting_history::json) AS (date text, muted bool, report_id uuid)
        ORDER BY 
            contact_uuid, 
            date
        ) AS mute_log
        LEFT JOIN {{ ref("contactview_metadata") }} contact
        ON mute_log.contact_uuid = contact.uuid
