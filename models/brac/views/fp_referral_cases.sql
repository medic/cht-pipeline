SELECT
    uuid,
    reported_by,
    reported_by_parent,
    source,
    record.source_id,
    patient_age_in_years,
    patient_age_in_months,
    patient_age_in_days,
    record.patient_id,
    patient_name,
    record.form,
    is_referral_case,
    NULLIF(fu.source_id, '') IS NOT NULL AS followed_up,
    referred_for_fp_method,
    fu.longterm_fp_received = 'iud' as iud,
    fu.longterm_fp_received = 'implant' as implant,
    fu.longterm_fp_received = 'depo_im' as depo_im,
    fu.longterm_fp_received = 'tl' as tubaligation,
    fu.longterm_fp_received = 'other' as other,
    referred_for_risks,
    fu.form AS followup_form,
    fu.reported as followup_date,
    record.reported
FROM {{ ref("formview_fp_patient_record") }} record
LEFT JOIN {{ ref("followup") }} fu ON fu.source_id = record.uuid AND record.patient_id = fu.patient_id
WHERE
    record.is_referral_case = 'true'