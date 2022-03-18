{% set time_now =  SELECT date_trunc('day', NOW()) } 

{% endset %}

{% set time_six_months = SELECT date_trunc('day', NOW() - interval '6 month') }

{% endset %}

{% set six_months_ago %}
SELECT

{{ get_hmis_data((time_now), (time_six_months)) }}

{% endset %}

{% set results = run_query(six_months_ago) %}

{{ log(results, info=True) }}

{{ return([]) }}

{% endset %}