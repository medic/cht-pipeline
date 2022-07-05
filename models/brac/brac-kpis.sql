(
    WITH ASSESSMENTS AS (
            SELECT 
                reported_by, 
                reported_by_parent,
                date_trunc('month',reported)::date AS period_reported,
                COUNT(uuid) AS count_all_assessments,
                SUM(COALESCE((malaria_treatment IS NOT NULL)::int,0)) AS u5_malaria_treatment,
                SUM(COALESCE((diarrhea_treatment IS NOT NULL)::int,0)) AS u5_diarrhea_treatment,
                SUM(COALESCE((pneumonia_treatment IS NOT NULL)::int,0)) AS u5_pneumonia_treatment,
                COUNT(uuid) FILTER (WHERE patient_age_in_months::int < 12 AND malaria_treatment IS NOT NULL) AS u1_malaria_treatment,
                COUNT(uuid) FILTER (WHERE patient_age_in_months::int < 12 AND diarrhea_treatment IS NOT NULL) AS u1_diarrhea_treatment,
                COUNT(uuid) FILTER (WHERE patient_age_in_months::int < 12 AND pneumonia_treatment IS NOT NULL) AS u1_pneumonia_treatment                
            FROM 
             {{ ref("useview_assessment") }}
            WHERE
             /* restrict patient age */
             patient_age_in_months >=2
              AND patient_age_in_months < 60
            GROUP BY
             reported_by,
             reported_by_parent,
             period_reported
        ) 
    SELECT
        anc.district_hospital_uuid AS branch_uuid,
        anc.district_hospital_name AS branch_name,
        anc.health_center_uuid AS area_uuid,
        anc.health_center_name AS area_name,
        chp.uuid AS chp_uuid,
        chp.name AS chp_name,
        --anc.clinic_uuid,
        --anc.clinic_name,
        anc.period_start,
        -- anc.period_start_epoch,
        anc.count_preg_4plus_visit_by_mdd AS count_4plus_anc,
        anc.count_preg_by_mdd AS count_preg,
        anc.percent_preg_4plus_visit_by_mdd AS percent_4plus_anc,
        anc.count_deliv_health_facility_by_deliv_date AS facility_delivery,
        anc.count_preg_early_reg_by_reg AS count_anc_first_trim,
        anc.count_preg_by_reg AS preg_count,
        anc.percent_preg_early_reg_by_reg AS percent_anc_first_trim,
        iccm.count_u5_ax AS count_u5_assessments,
        ASSESSMENTS.count_all_assessments AS count_all_assessments,
        safe_divide(iccm.count_u5_ax,COALESCE(ASSESSMENTS.count_all_assessments,0),3) AS percent_assessments,
        iccm.count_ax_within_24 AS count_assessments_within_24,
        iccm.count_ax_with_fu_ref_complete_attend_hf AS count_assessments_with_fu_ref_complete_attend_hf,
        iccm.count_malaria_dx AS u5_malaria_diagnoses,
        iccm.count_diarrhea_dx AS u5_diarrhea_diagnoses,
        iccm.count_pneumonia_dx AS u5_pneumonia_diagnoses,
        iccm.count_total_dx AS count_u5_total_dx,
        COALESCE(ASSESSMENTS.u5_malaria_treatment,0) AS u5_malaria_treatment,
        COALESCE(ASSESSMENTS.u5_diarrhea_treatment,0) AS u5_diarrhea_treatment,
        COALESCE(ASSESSMENTS.u5_pneumonia_treatment,0) AS u5_pneumonia_treatment,
        u1_iccm.count_malaria_dx AS u1_malaria_diagnoses,
        u1_iccm.count_diarrhea_dx AS u1_diarrhea_diagnoses,
        u1_iccm.count_pneumonia_dx AS u1_pneumonia_diagnoses,
        u1_iccm.count_total_dx AS count_u1_total_dx,
        COALESCE(ASSESSMENTS.u1_malaria_treatment,0) AS u1_malaria_treatment,
        COALESCE(ASSESSMENTS.u1_diarrhea_treatment,0) AS u1_diarrhea_treatment,
        COALESCE(ASSESSMENTS.u1_pneumonia_treatment,0) AS u1_pneumonia_treatment,
        hh.total_hh_registered,
        hh.hh_total_visit,
        hh.hh_visit AS unique_households_visited,
        hh.percent_hh_visit AS percent_hh_visit
    FROM
        {{ ref("get_dashboard_data_anc_impact" ) }}('health_center','12','month','true') anc
        LEFT JOIN {{ ref("contactview_chp") }} chp 
        ON
        anc.health_center_uuid = chp.area_uuid 
        LEFT JOIN {{ ref("get_dashboard_data_iccm_impact") }}('health_center','12','month','true') iccm
        ON
        anc.health_center_uuid=iccm.health_center_uuid 
        AND anc.period_start = iccm.period_start
        LEFT JOIN {{ ref("get_dashboard_data_iccm_impact_u1") }}('health_center','12','month','true') u1_iccm
        ON 
        anc.health_center_uuid=u1_iccm.health_center_uuid 
        AND anc.period_start = u1_iccm.period_start
        LEFT JOIN {{ ref("get_dashboard_data_hh_brac") }}('health_center','12','month','true') hh
        ON 
        anc.health_center_uuid=hh.health_center_uuid 
        AND anc.period_start = hh.period_start
        LEFT JOIN ASSESSMENTS
        ON
        anc.health_center_uuid = ASSESSMENTS.reported_by_parent 
        AND anc.period_start = ASSESSMENTS.period_reported
        WHERE chp.uuid <> ''
    ORDER BY 
        anc.district_hospital_name, 
        anc.health_center_name,
        anc.clinic_name,
        anc.period_start
)