{% set six_months_ago %}
SELECT

{{get_hmis_data((date_trunc('day', NOW())), (date_trunc('day', NOW() - interval '6 month')))}}

{% endset %}

{% set results = run_query(six_months_ago) %}

{{ log(results, info=True) }}

{{ return([]) }}

{% endset %}