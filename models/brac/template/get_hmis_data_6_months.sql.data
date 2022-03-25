{% set time_now = dbt_utils.current_timestamp() -%}


{% set time_six_months_ago = dbt_date.n_months_ago(6) -%}

{% set get_hmis_data_query = get_hmis_data(
    startDate=time_six_months_ago, 
    endDate=time_now
)

-%}

{% set results = run_query(get_hmis_data_query) %}

{{ log(results, info=True) }}
