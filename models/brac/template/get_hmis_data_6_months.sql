{% set time_now =  dbt_date.now("America/New_York") %} 

{% set time_six_months_ago = dbt_date.n_months_ago(6, tz="America/New_York") %}

{% set results = time_six_months_ago %}

{{ log(results, info=True) }}