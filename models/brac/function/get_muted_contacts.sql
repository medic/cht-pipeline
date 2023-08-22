{{ config(materialized = 'raw_sql') }}

CREATE OR REPLACE FUNCTION {{this}}(to_date TIMESTAMP WITH TIME ZONE, contact_type text)
RETURNS TABLE(
    contact_uuid text,
    type text,
    parent_uuid text,
    muted_on TIMESTAMP WITH TIME ZONE,
    unmuted_on TIMESTAMP WITH TIME ZONE
)
AS
$BODY$
(
    WITH MUTING_CTE AS (
        SELECT
            contact_uuid,
            "type",
            parent_uuid,
            mute_status,
            "date" AS muted_on,
            LEAD(DATE,1) OVER (PARTITION BY contact_uuid ORDER BY DATE ASC)::TIMESTAMP WITH TIME ZONE AS unmuted_on
        FROM {{ ref("contactview_muted") }}
        WHERE contact_type = 'All' OR "type" = contact_type
    )

    SELECT
        contact_uuid,
        "type",
        parent_uuid,
        muted_on,
        unmuted_on
    FROM
        MUTING_CTE
    WHERE
        mute_status IS TRUE
        AND muted_on < (date_trunc('day',to_date) + '1 day'::interval)
        AND (unmuted_on IS NULL OR unmuted_on >= (date_trunc('day',to_date) + '1 day'::interval))
    ORDER BY contact_uuid
)
$BODY$

LANGUAGE 'sql' STABLE;