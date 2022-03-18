{% set time_now =  dbt_utils.current_timestamp() -%} 

{% set time_six_months_ago = dbt_utils.current_timestamp() -%}

{% set results = time_now %}

{{ log(results, info=True) }}