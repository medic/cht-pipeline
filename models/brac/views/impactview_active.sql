SELECT month_facility.month,
    month_facility.epoch,
    month_facility.facility_name,
    sum(
        CASE
            WHEN ((EXISTS ( SELECT 1
               FROM {{ ref("form_metadata") }} meta
              WHERE ((meta.chw = chw_facility.chw_uuid) AND ((date_trunc('month'::text, meta.reported))::date = month_facility.month)))) OR (EXISTS ( SELECT 1
               FROM (({{ ref("contactview_metadata") }} person
                 JOIN {{ ref("contactview_metadata") }} family ON ((person.parent_uuid = family.uuid)))
                 JOIN {{ ref("contactview_metadata") }} chw_area ON ((family.parent_uuid = chw_area.uuid)))
              WHERE (((family.type = 'clinic'::text) AND (chw_area.contact_uuid = chw_facility.chw_uuid)) AND ((date_trunc('month'::text, person.reported))::date = month_facility.month))))) THEN 1
            ELSE 0
        END) AS active_chws
FROM ({{ ref("impactview_month_facility") }} month_facility
    LEFT JOIN {{ ref("impactview_chw_facility") }} chw_facility ON ((month_facility.facility_join_field = chw_facility.facility_join_field)))
GROUP BY month_facility.month, month_facility.epoch, month_facility.facility_name, month_facility.facility_join_field
ORDER BY month_facility.month, month_facility.epoch, month_facility.facility_name