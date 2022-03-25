{% set time_now = dbt_utils.current_timestamp() -%}


{% set time_six_months_ago = dbt_date.n_months_ago(2) -%}

{% set get_assessment_query = get_assessment_data(
    startDate=time_six_months_ago, 
    endDate=time_now
)

-%}

{% set results = run_query(get_assessment_query) %}

{% if execute %}
{# Return the first column #}
{% set uuid_results_list = results.columns[0].values() %}
{% else %}
{% set uuid_results_list = [] %}
{% endif %}

{{ log(uuid_results_list, info=True) }}
