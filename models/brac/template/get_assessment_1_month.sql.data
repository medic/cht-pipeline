{{
    config(
        materialized = 'incremental',
        unique_key='uuid',
        indexes=[
            {'columns': ['reported']},
            {'columns': ['reported_by_parent']}
        ]
    )
}}

{% set time_now = dbt_utils.current_timestamp() -%}


{% set time_six_months_ago = dbt_date.n_months_ago(2) -%}

{{get_assessment_data(
    startDate=time_six_months_ago, 
    endDate=time_now
)}}