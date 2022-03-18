{% set time_now =  dbt_date.now() %} 

{% set time_six_months_ago = dbt_date.n_months_ago(6) %}

{% set results = time_six_months_ago %}

{{ log(results, info=True) }}

{{ return([]) }}