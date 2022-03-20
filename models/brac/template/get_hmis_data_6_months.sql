{% set time_now =  dbt_utils.get_query_results_as_dict("SELECT DATE_TRUNC('month', NOW())") -%} 

{% set time_six_months_ago = dbt_utils.get_query_results_as_dict("SELECT DATE_TRUNC('month', NOW() - interval '6 month')") -%}

{% set results = time_now %}

{{ log(results, info=True) }}