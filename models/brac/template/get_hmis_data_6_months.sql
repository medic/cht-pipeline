{% set six_months_ago %}
SELECT

{{get_hmis_data('now'::timestamp, ('now'::timestamp - '6 month'::interval))}}

{% endset %}

{% set results = run_query(six_months_ago) %}

{{ log(results, info=True) }}