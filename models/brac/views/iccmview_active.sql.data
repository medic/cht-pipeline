SELECT month_facility.month,
    month_facility.epoch,
    month_facility.facility_name,
    sum(
        CASE
            WHEN ((EXISTS ( SELECT 1
               FROM {{ ref("useview_assessment") }} assess
              WHERE (((assess.chw = chw_facility.chw_uuid) AND ((date_trunc('month'::text, assess.reported))::date = month_facility.month)) AND (assess.patient_age_in_years < 5)))) OR (EXISTS ( SELECT 1
               FROM {{ ref("useview_assessment_follow_up") }} assess_fu
              WHERE (((assess_fu.chw = chw_facility.chw_uuid) AND ((date_trunc('month'::text, assess_fu.reported))::date = month_facility.month)) AND (assess_fu.patient_age_in_years < 5))))) THEN 1
            ELSE 0
        END) AS active_chws
FROM ({{ ref("impactview_month_facility") }} month_facility
    LEFT JOIN {{ ref("impactview_chw_facility") }} chw_facility ON ((month_facility.facility_join_field = chw_facility.facility_join_field)))
GROUP BY month_facility.month, month_facility.epoch, month_facility.facility_name, month_facility.facility_join_field
ORDER BY month_facility.month, month_facility.epoch, month_facility.facility_name