SELECT 
                    source_id,
                    patient_id,
                    form,
                    longterm_fp_received,
                    reported
                FROM {{ ref("formview_fp_patient_record") }}