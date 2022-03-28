{{
    config(
        materialized = 'incremental',
        indexes=[
            {'columns': ['chp_uuid']},
            {'columns': ['branch_uuid']},
            {'columns': ['supervisor_uuid']}
        ]
    )
}}


{% set time_now = dbt_utils.current_timestamp() -%}


{% set time_six_months_ago = dbt_date.n_months_ago(6) -%}

{{ get_hmis_data(
    startDate=time_six_months_ago, 
    endDate=time_now
)}}