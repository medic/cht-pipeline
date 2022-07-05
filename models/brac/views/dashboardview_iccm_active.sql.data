SELECT month_facility.month,
    month_facility.epoch,
    month_facility.facility_name,
    month_facility.facility_join_field,
    sum(impactview_active.active_chws) AS all_forms_active_chws,
    sum(iccmview_active.active_chws) AS iccm_active_chws
FROM (({{ ref("impactview_month_facility") }} month_facility
    LEFT JOIN {{ ref("iccmview_active") }} ON (((month_facility.month = iccmview_active.month) AND (month_facility.facility_name = iccmview_active.facility_name))))
    LEFT JOIN {{ ref("impactview_active") }} ON (((month_facility.month = impactview_active.month) AND (month_facility.facility_name = impactview_active.facility_name))))
GROUP BY month_facility.month, month_facility.epoch, month_facility.facility_name, month_facility.facility_join_field
ORDER BY month_facility.month, month_facility.facility_name