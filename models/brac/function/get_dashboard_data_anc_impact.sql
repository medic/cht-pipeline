{{ config(materialized = 'raw_sql') }}  
CREATE OR REPLACE FUNCTION {{ this }} (param_facility_group_by text, param_num_units text default '12', param_interval_unit text default 'month', param_include_current boolean default 'true')

RETURNS TABLE(
    district_hospital_uuid text,
    district_hospital_name text,
    health_center_uuid text,
    health_center_name text,
    clinic_uuid text,
    clinic_name text,
    period_start date,
    period_start_epoch numeric,
    facility_join_field text,
    
    count_new_person_by_reg numeric,
    count_preg_by_reg numeric,
    count_preg_with_lmp_by_reg numeric,
    count_preg_without_lmp_by_reg numeric,
    count_preg_early_reg_by_reg numeric,
    count_deliv_by_reg numeric,
    count_danger_sign_by_reg numeric,
    count_pregnancy_visit_by_reg numeric,
    percent_preg_early_reg_by_reg double precision,
    
    count_preg_by_mdd numeric,
    count_preg_with_lmp_by_mdd numeric,
    count_preg_without_lmp_by_mdd numeric,
    count_preg_confirmed_deliv_by_mdd numeric,
    count_preg_1plus_visit_by_mdd numeric,
    count_preg_4plus_visit_by_mdd numeric,
    count_preg_early_reg_by_mdd numeric,
    count_preg_early_reg_4plus_visit_by_mdd numeric,
    count_preg_danger_sign_by_mdd numeric,
    count_preg_danger_sign_4plus_visit_by_mdd numeric,
    percent_deliv_confirmed_by_mdd double precision,
    percent_preg_1plus_visit_by_mdd double precision,
    percent_preg_4plus_visit_by_mdd double precision,
    percent_preg_early_reg_4plus_visits_by_mdd double precision,
    percent_preg_danger_sign_4plus_visits_by_mdd double precision,
    
    count_preg_by_2nd_tri_end numeric,
    count_preg_1plus_visits_by_2nd_tri_end numeric,
    count_preg_2plus_visits_by_2nd_tri_end numeric,
    count_preg_3plus_visits_by_2nd_tri_end numeric,
    percent_preg_1plus_visits_by_2nd_tri_end double precision,
    percent_preg_2plus_visits_by_2nd_tri_end double precision,
    percent_preg_3plus_visits_by_2nd_tri_end double precision,
    
    count_deliv_by_deliv_date numeric,
    count_deliv_health_facility_by_deliv_date numeric,
    count_deliv_skilled_care_by_deliv_date numeric,
    count_deliv_early_reg_by_deliv_date numeric,
    count_deliv_health_facility_early_reg_by_deliv_date numeric,
    count_deliv_danger_sign_by_deliv_date numeric,
    count_deliv_health_facility_danger_sign_by_deliv_date numeric,
    percent_deliv_health_facility_by_deliv_date double precision,
    percent_deliv_skilled_care_by_deliv_date double precision,
    percent_deliv_early_reg_health_facility_by_deliv_date double precision,
    percent_deliv_danger_sign_health_facility_by_deliv_date double precision,
    
    count_reported_by numeric
) 
LANGUAGE sql
STABLE	
    
AS
$BODY$

    
WITH period_CTE AS
(
    SELECT generate_series(
        date_trunc(param_interval_unit,now() - (param_num_units||' '||param_interval_unit)::interval), 
                                    
        CASE
        WHEN param_include_current 
        THEN now() 
        ELSE now() - ('1 ' || param_interval_unit)::interval
        END, 
                                    
        ('1 '||param_interval_unit)::interval
        )::date AS start

), anc_active_config_CTE AS 
(
    SELECT
    ((value->>'active'))::jsonb AS anc_forms
                
    FROM
    {{ ref("configuration") }}
    WHERE
    key = 'anc'
    AND value ? 'active'
    ), pregnancy_CTE AS
    (
    SELECT
    preg.uuid,
    preg.patient_id,
    preg.reported_by_parent,
    date_trunc(param_interval_unit,preg.reported)::date AS period_reported,
    date_trunc(param_interval_unit,preg.second_tri_end)::date AS period_2nd_tri_end,
    date_trunc(param_interval_unit,preg.mdd)::date AS period_mdd,
    preg.has_lmp,
    preg.early_reg,
    preg.danger_sign_at_reg AS danger_sign,
    count(visit.*) AS count_visit_total,
    COALESCE(SUM((date_trunc('day',visit.reported) <= preg.second_tri_end)::int),0) AS  count_visit_by_2nd_tri_end				
    FROM
    {{ ref("ancview_pregnancy") }} AS preg
    LEFT JOIN {{ ref("ancview_pregnancy_visit") }} AS visit ON (preg.uuid = visit.pregnancy_id)				
            
    GROUP BY
    preg.uuid,
    preg.patient_id,
    preg.reported_by_parent,
    period_reported,
    period_2nd_tri_end,
    period_mdd,
    preg.early_reg,
    preg.danger_sign_at_reg,
    preg.has_lmp
                
)
        
--######################
--MAIN QUERY STARTS HERE
--######################

/*parameterized group by*/
/*this version is specifically for LG config where 'clinic' shouldn't be a group by category...lowest level is health_center' */
SELECT

    CASE
        WHEN param_facility_group_by = 'clinic' OR param_facility_group_by = 'health_center' OR param_facility_group_by = 'district_hospital'
        THEN place_period.district_hospital_uuid
        ELSE 'All'
    END AS _district_hospital_uuid,	
    CASE
        WHEN param_facility_group_by = 'clinic' OR param_facility_group_by = 'health_center' OR param_facility_group_by = 'district_hospital'
        THEN place_period.district_hospital_name
        ELSE 'All'
    END AS _district_hospital_name,
    
    CASE
        WHEN param_facility_group_by = 'clinic' OR param_facility_group_by = 'health_center'
        THEN place_period.health_center_uuid
        ELSE 'All'
    END AS _health_center_uuid,			
    CASE
        WHEN param_facility_group_by = 'clinic' OR param_facility_group_by = 'health_center'
        THEN place_period.health_center_name
        ELSE 'All'
    END AS _health_center_name,
    
    'clinic_uuid'::text AS _clinic_uuid,
    'clinic_name'::text AS _clinic_name,
    
    place_period.period_start AS _period_start,
    date_part('epoch',place_period.period_start)::numeric AS _period_start_epoch,
    
    CASE
        WHEN param_facility_group_by = 'health_center'
            THEN place_period.health_center_uuid
        WHEN param_facility_group_by = 'district_hospital'
            THEN place_period.district_hospital_uuid
        ELSE
            'All'
    END AS _facility_join_field,
        
        /* By Reg Date */
        SUM(COALESCE(count_new_person_by_reg,0)) AS count_new_person_by_reg,
        SUM(COALESCE(count_preg_by_reg,0)) AS count_preg_by_reg,
        SUM(COALESCE(count_preg_with_lmp_by_reg,0)) AS count_preg_with_lmp_by_reg,
        SUM(COALESCE(count_preg_without_lmp_by_reg,0)) AS count_preg_without_lmp_by_reg,
        
        
        SUM(COALESCE(count_preg_early_reg_by_reg,0)) AS count_preg_early_reg_by_reg,
        SUM(COALESCE(count_deliv_by_reg,0)) AS count_deliv_by_reg,
        SUM(COALESCE(count_danger_sign_by_reg,0)) AS count_danger_sign_by_reg,
        SUM(COALESCE(count_pregnancy_visit_by_reg,0)) AS count_pregnancy_visit_by_reg,
    
        CASE
            WHEN SUM(COALESCE(count_preg_by_reg,0)) = 0
            THEN 0::float
            ELSE (SUM(COALESCE(count_preg_early_reg_by_reg,0))::float / SUM(COALESCE(count_preg_by_reg,0))::float) * 100            
        END AS percent_preg_early_reg_by_reg,
        
        /* By MDD */
        
        SUM(COALESCE(count_preg_by_mdd,0)) AS count_preg_by_mdd,
        SUM(COALESCE(count_preg_with_lmp_by_mdd,0)) AS count_preg_with_lmp_by_mdd,
        SUM(COALESCE(count_preg_without_lmp_by_mdd,0)) AS count_preg_without_lmp_by_mdd,
        SUM(COALESCE(count_preg_confirmed_deliv_by_mdd,0)) AS count_preg_confirmed_deliv_by_mdd,
        SUM(COALESCE(count_preg_1plus_visit_by_mdd,0)) AS count_preg_1plus_visit_by_mdd,    
        SUM(COALESCE(count_preg_4plus_visit_by_mdd,0)) AS count_preg_4plus_visit_by_mdd,    
        SUM(COALESCE(count_preg_early_reg_by_mdd,0)) AS count_preg_early_reg_by_mdd,
        SUM(COALESCE(count_preg_early_reg_4plus_visit_by_mdd,0)) AS count_preg_early_reg_4plus_visit_by_mdd,
        SUM(COALESCE(count_preg_danger_sign_by_mdd,0)) AS count_preg_danger_sign_by_mdd,
        SUM(COALESCE(count_preg_danger_sign_4plus_visit_by_mdd,0)) AS count_preg_danger_sign_4plus_visit_by_mdd,    
        
        CASE
            WHEN SUM(COALESCE(count_preg_by_mdd,0)) = 0
            THEN 0::float
            ELSE (SUM(COALESCE(count_preg_confirmed_deliv_by_mdd,0))::float / SUM(COALESCE(count_preg_by_mdd,0))::float) * 100          
        END AS percent_deliv_confirmed_by_mdd,
    
        CASE
            WHEN SUM(COALESCE(count_preg_by_mdd,0)) = 0
            THEN 0::float
            ELSE (SUM(COALESCE(count_preg_1plus_visit_by_mdd,0))::float / SUM(COALESCE(count_preg_by_mdd,0))::float) * 100          
        END AS percent_preg_1plus_visit_by_mdd,
    
        CASE
            WHEN SUM(COALESCE(count_preg_by_mdd,0)) = 0
            THEN 0::float
            ELSE (SUM(COALESCE(count_preg_4plus_visit_by_mdd,0))::float / SUM(COALESCE(count_preg_by_mdd,0))::float) * 100          
        END AS percent_preg_4plus_visit_by_mdd,
    
        CASE
            WHEN SUM(COALESCE(count_preg_early_reg_by_mdd,0)) = 0
            THEN 0::float
            ELSE (SUM(COALESCE(count_preg_early_reg_4plus_visit_by_mdd,0))::float / SUM(COALESCE(count_preg_early_reg_by_mdd,0))::float) * 100          
        END AS percent_preg_early_reg_4plus_visits_by_mdd,
    
        CASE
            WHEN SUM(COALESCE(count_preg_danger_sign_by_mdd,0)) = 0
            THEN 0::float
            ELSE (SUM(COALESCE(count_preg_danger_sign_4plus_visit_by_mdd,0))::float / SUM(COALESCE(count_preg_danger_sign_by_mdd,0))::float) * 100          
        END AS percent_preg_danger_sign_4plus_visits_by_mdd,
            
        /* By 2nd Trimester End */
        
        SUM(COALESCE(count_preg_by_2nd_tri_end,0)) AS count_preg_by_2nd_tri_end,
        SUM(COALESCE(count_preg_1plus_visits_by_2nd_tri_end,0)) AS count_preg_1plus_visits_by_2nd_tri_end,
        SUM(COALESCE(count_preg_2plus_visits_by_2nd_tri_end,0)) AS count_preg_2plus_visits_by_2nd_tri_end,
        SUM(COALESCE(count_preg_3plus_visits_by_2nd_tri_end,0)) AS count_preg_3plus_visits_by_2nd_tri_end,
    
        CASE
            WHEN SUM(COALESCE(count_preg_by_2nd_tri_end,0)) = 0
            THEN 0::float
            ELSE (SUM(COALESCE(count_preg_1plus_visits_by_2nd_tri_end,0))::float / SUM(COALESCE(count_preg_by_2nd_tri_end,0))::float) * 100         
        END AS percent_preg_1plus_visits_by_2nd_tri_end,
    
        CASE
            WHEN SUM(COALESCE(count_preg_by_2nd_tri_end,0)) = 0
            THEN 0::float
            ELSE (SUM(COALESCE(count_preg_2plus_visits_by_2nd_tri_end,0))::float / SUM(COALESCE(count_preg_by_2nd_tri_end,0))::float) * 100         
        END AS percent_preg_2plus_visits_by_2nd_tri_end,
    
        CASE
            WHEN SUM(COALESCE(count_preg_by_2nd_tri_end,0)) = 0
            THEN 0::float
            ELSE (SUM(COALESCE(count_preg_3plus_visits_by_2nd_tri_end,0))::float / SUM(COALESCE(count_preg_by_2nd_tri_end,0))::float) * 100         
        END AS percent_preg_3plus_visits_by_2nd_tri_end,    
        
        /* By Delivery Date */
        
        SUM(COALESCE(count_deliv_by_deliv_date,0)) AS count_deliv_by_deliv_date,
        SUM(COALESCE(count_deliv_health_facility_by_deliv_date,0)) AS count_deliv_health_facility_by_deliv_date,
        SUM(COALESCE(count_deliv_skilled_care_by_deliv_date,0)) AS count_deliv_skilled_care_by_deliv_date,
        SUM(COALESCE(count_deliv_early_reg_by_deliv_date,0)) AS count_deliv_early_reg_by_deliv_date,
        SUM(COALESCE(count_deliv_health_facility_early_reg_by_deliv_date,0)) AS count_deliv_health_facility_early_reg_by_deliv_date,
        SUM(COALESCE(count_deliv_danger_sign_by_deliv_date,0)) AS count_deliv_danger_sign_by_deliv_date,
        SUM(COALESCE(count_deliv_health_facility_danger_sign_by_deliv_date,0)) AS count_deliv_health_facility_danger_sign_by_deliv_date,
            
        CASE
            WHEN SUM(COALESCE(count_deliv_by_deliv_date,0)) = 0
            THEN 0::float
            ELSE (SUM(COALESCE(count_deliv_health_facility_by_deliv_date,0))::float / SUM(COALESCE(count_deliv_by_deliv_date,0))::float) * 100          
        END AS percent_deliv_health_facility_by_deliv_date, 
        
        CASE
            WHEN SUM(COALESCE(count_deliv_by_deliv_date,0)) = 0
            THEN 0::float
            ELSE (SUM(COALESCE(count_deliv_skilled_care_by_deliv_date,0))::float / SUM(COALESCE(count_deliv_by_deliv_date,0))::float) * 100         
        END AS percent_deliv_skilled_care_by_deliv_date,    
        
        CASE
            WHEN SUM(COALESCE(count_deliv_early_reg_by_deliv_date,0)) = 0
            THEN 0::float
            ELSE (SUM(COALESCE(count_deliv_health_facility_early_reg_by_deliv_date,0))::float / SUM(COALESCE(count_deliv_early_reg_by_deliv_date,0))::float) * 100            
        END AS percent_deliv_early_reg_health_facility_by_deliv_date,   
        
        CASE
            WHEN SUM(COALESCE(count_deliv_danger_sign_by_deliv_date,0)) = 0
            THEN 0::float
            ELSE (SUM(COALESCE(count_deliv_health_facility_danger_sign_by_deliv_date,0))::float / SUM(COALESCE(count_deliv_danger_sign_by_deliv_date,0))::float) * 100          
        END AS percent_deliv_danger_sign_health_facility_by_deliv_date,
        
        SUM(COALESCE(health_center.count_reported_by,0)) AS count_reported_by
    
FROM /*combination of hierarchy level and time-periods we are grouping by (last 12 mo, by branch)*/
        (
        SELECT
            district_hospital.uuid AS district_hospital_uuid,
            district_hospital.name AS district_hospital_name,
            health_center.uuid AS health_center_uuid,
            health_center.name AS health_center_name,
            period_CTE.start AS period_start
            
        FROM
            period_CTE,
            {{ ref("contactview_metadata") }} AS health_center
            INNER JOIN {{ ref("contactview_metadata") }} AS district_hospital ON (health_center.parent_uuid = district_hospital.uuid)
                    
        WHERE
            district_hospital.type = 'district_hospital'
                 
    ) AS place_period
            
        
        
    LEFT JOIN /* Pregnancies by Reported Date */ 
    (
        SELECT
            preg.reported_by_parent,
            date_trunc(param_interval_unit,preg.period_reported)::date AS period_reported,

            COUNT(preg.uuid) AS count_preg_by_reg,
            COUNT(preg.uuid) AS count_preg_with_lmp_by_reg,
            SUM((NOT preg.has_lmp)::int) AS count_preg_without_lmp_by_reg,
            SUM((preg.early_reg)::int) AS count_preg_early_reg_by_reg
            
        FROM
            pregnancy_CTE AS preg			
        GROUP BY
            preg.reported_by_parent,
            period_reported
        ORDER BY
            period_reported		
        ) AS preg_by_reported ON (place_period.period_start = preg_by_reported.period_reported AND place_period.health_center_uuid = preg_by_reported.reported_by_parent)
                                                                        
    
    LEFT JOIN /* Pregnancies by MDD */
    
    (
        SELECT
            preg.reported_by_parent,
            preg.period_mdd,
            
            COUNT(preg.uuid) AS count_preg_by_mdd,
            COUNT(preg.uuid) AS count_preg_with_lmp_by_mdd,
            SUM((NOT preg.has_lmp)::int) AS count_preg_without_lmp_by_mdd,
            
            SUM((deliv.uuid IS NOT NULL)::int) AS count_preg_confirmed_deliv_by_mdd,
            
            SUM((preg.early_reg)::int) AS count_preg_early_reg_by_mdd,
            SUM((preg.danger_sign)::int) AS count_preg_danger_sign_by_mdd,
            
            SUM((preg.count_visit_total >=1)::int) AS count_preg_1plus_visit_by_mdd,
            SUM((preg.count_visit_total >=4)::int) AS count_preg_4plus_visit_by_mdd,
            
            SUM((preg.count_visit_total >=4 AND preg.early_reg)::int) AS count_preg_early_reg_4plus_visit_by_mdd,
            SUM((preg.count_visit_total >=4 AND preg.danger_sign)::int) AS count_preg_danger_sign_4plus_visit_by_mdd
                
        FROM
            pregnancy_CTE AS preg
            LEFT JOIN {{ ref("ancview_delivery") }} AS deliv ON (deliv.pregnancy_id = preg.uuid)
        GROUP BY
            preg.reported_by_parent,
            preg.period_mdd				
            
    ) AS preg_by_mdd ON (place_period.period_start = preg_by_mdd.period_mdd AND place_period.health_center_uuid = preg_by_mdd.reported_by_parent)
        
    
    LEFT JOIN /* Pregnancies by 2nd Trimester End */	
    (
        SELECT
            reported_by_parent,
            period_2nd_tri_end,			
            COUNT(uuid) AS count_preg_by_2nd_tri_end,
            SUM((count_visit_by_2nd_tri_end >= 1 AND has_lmp)::int) AS count_preg_1plus_visits_by_2nd_tri_end,
            SUM((count_visit_by_2nd_tri_end >= 2 AND has_lmp)::int) AS count_preg_2plus_visits_by_2nd_tri_end,
            SUM((count_visit_by_2nd_tri_end >= 3 AND has_lmp)::int) AS count_preg_3plus_visits_by_2nd_tri_end				
        FROM
            pregnancy_CTE 			
        GROUP BY
            reported_by_parent,
            period_2nd_tri_end  	
    ) AS preg_by_2nd_tri_end ON (place_period.period_start = preg_by_2nd_tri_end.period_2nd_tri_end AND place_period.health_center_uuid = preg_by_2nd_tri_end.reported_by_parent)
                                                                
        LEFT JOIN /* Deliveries by Delivery Date */
            (
                SELECT
                    deliv.reported_by_parent,
                    date_trunc(param_interval_unit,delivery_date)::date AS period_delivery_date,
        
                    count(deliv.*) AS count_deliv_by_deliv_date,
                    SUM((deliv.at_health_facility)::int) AS count_deliv_health_facility_by_deliv_date,
                    SUM((deliv.skilled_birth_attendant)::int) AS count_deliv_skilled_care_by_deliv_date,
                    SUM((preg.early_reg)::int) AS count_deliv_early_reg_by_deliv_date,
                    SUM((preg.early_reg AND deliv.at_health_facility)::int) AS count_deliv_health_facility_early_reg_by_deliv_date,
                    SUM((preg.danger_sign_at_reg)::int) AS count_deliv_danger_sign_by_deliv_date,
                    SUM((preg.danger_sign_at_reg AND deliv.at_health_facility)::int) AS count_deliv_health_facility_danger_sign_by_deliv_date							
                FROM
                    {{ ref("ancview_delivery") }} AS deliv
                    LEFT JOIN {{ ref("ancview_pregnancy") }} AS preg ON (preg.uuid = deliv.pregnancy_id)				
                WHERE
                    deliv.delivered
                    AND deliv.reported >= now() - (param_num_units||' '||param_interval_unit)::interval
    
                GROUP BY
                    deliv.reported_by_parent,
                    period_delivery_date
    
            ) AS deliv_by_deliv_date ON (place_period.period_start = deliv_by_deliv_date.period_delivery_date AND place_period.health_center_uuid = deliv_by_deliv_date.reported_by_parent)
            
            
            LEFT JOIN
                (
                    SELECT
                        deliv.reported_by_parent AS reported_by_parent,
                        date_trunc(param_interval_unit,deliv.reported) AS period_reported,						
                        count(deliv.*) AS count_deliv_by_reg						
                    FROM
                        {{ ref("ancview_delivery") }} AS deliv					
                    WHERE
                        deliv.reported >= now() - (param_num_units||' '||param_interval_unit)::interval										
                    GROUP BY
                        reported_by_parent,
                        period_reported
    
                ) AS deliv_by_reg ON (place_period.period_start = deliv_by_reg.period_reported AND place_period.health_center_uuid = deliv_by_reg.reported_by_parent)
    
            LEFT JOIN
                (
                    SELECT
                        ds.reported_by_parent AS reported_by_parent,
                        date_trunc(param_interval_unit,ds.reported) AS period_reported,
                        count(ds.*) AS count_danger_sign_by_reg						
                    FROM
                        {{ ref("ancview_danger_sign") }} AS ds
                    
                    WHERE
                        ds.reported >= now() - (param_num_units||' '||param_interval_unit)::interval					
                    GROUP BY
                        reported_by_parent,
                        period_reported
    
                ) AS danger_sign_by_reg ON (place_period.period_start = danger_sign_by_reg.period_reported AND place_period.health_center_uuid = danger_sign_by_reg.reported_by_parent)	
            LEFT JOIN
                (
                    SELECT
                        visit.reported_by_parent AS reported_by_parent,
                        date_trunc(param_interval_unit,visit.reported) AS period_reported,

                        count(visit.*) AS count_pregnancy_visit_by_reg
                        
                    FROM
                        {{ ref("ancview_pregnancy_visit") }} AS visit
                    
                    WHERE
                        visit.reported >= now() - (param_num_units||' '||param_interval_unit)::interval
                    
                    GROUP BY
                        reported_by_parent,
                        period_reported
    
                ) AS preg_visit_by_reg ON (place_period.period_start = preg_visit_by_reg.period_reported AND place_period.health_center_uuid = preg_visit_by_reg.reported_by_parent)	
            LEFT JOIN
                (
                    SELECT
                        person.parent_uuid AS reported_by_parent,
                        date_trunc(param_interval_unit,person.reported) AS period_reported,
                    
                        count(*) AS count_new_person_by_reg
                        
                    FROM
                        {{ ref("contactview_metadata") }} AS person
                        INNER JOIN {{ ref("contactview_metadata") }} AS parent ON (person.parent_uuid = parent.uuid AND person.uuid <> parent.contact_uuid)
                    
                    WHERE
                        person.reported >= now() - (param_num_units||' '||param_interval_unit)::interval
                    
                    GROUP BY
                        person.parent_uuid,
                        period_reported             
                
                ) AS new_person_by_reg ON (place_period.period_start = new_person_by_reg.period_reported AND place_period.health_center_uuid = new_person_by_reg.reported_by_parent)	

            LEFT JOIN
                (
                    SELECT
                        fm.reported_by_parent AS reported_by_parent,
                        date_trunc(param_interval_unit,fm.reported) AS period_reported,
                        
                        count(distinct(fm.reported_by)) AS count_reported_by
                    
                    FROM 
                        anc_active_config_CTE AS config,
                        {{ ref("form_metadata") }} AS fm
                                            
                    WHERE
                        config.anc_forms @> format('"%s"',form)::jsonb
                        AND fm.reported >= now () - (param_num_units||' '||param_interval_unit)::interval
                        
                    GROUP BY
                        fm.reported_by_parent,
                        period_reported
                
                ) AS health_center ON (place_period.period_start = health_center.period_reported AND place_period.health_center_uuid = health_center.reported_by_parent)
            
    
    GROUP BY
        
        _district_hospital_uuid,
        _district_hospital_name,
        _health_center_uuid,
        _health_center_name,
        _clinic_uuid,
        _clinic_name,
        _period_start,
        _period_start_epoch,
        _facility_join_field
        
    ORDER BY
    
        _district_hospital_name,
        _health_center_name,
        _clinic_name,
        _period_start	
$BODY$