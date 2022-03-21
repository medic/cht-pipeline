\set time_now NOW()

\set time_six_months_ago (date_trunc('day', NOW() - interval '6 month'))

\set six_months_ago = SELECT {{ get_hmis_data(NOW(), (date_trunc('day', NOW() - interval '6 month'))) }}

\set results = run_query(six_months_ago)

{{ log(results, info=True) }}