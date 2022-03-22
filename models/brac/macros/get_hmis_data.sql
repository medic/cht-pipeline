{% macro get_hmis_data(startDate, endDate) %}

{% set get_hmis_data_query %}
SELECT
	period_chp.uuid AS chp_uuid
	
	
FROM
	(
		SELECT
			chp.area_uuid,
			chp.uuid,
			chp.name,
			chp.branch_name,
			chp.branch_uuid,
			chp.supervisor_uuid,
			chp.region,	
			generate_series(date_trunc('month',({{startDate}})::date), 
							date_trunc('month',({{endDate}})::date), 
							'1 month'::interval
							)::DATE AS date
		FROM
			{{ ref("contactview_chp") }} AS chp
	) AS period_chp
{% endset %}

{% set results = run_query(get_hmis_data_query)}

{% if execute %}
{% set result_list=results.columns[0].values() %}
{% else %}
{% set results_list	= [] %}

{% endif %}

{% endmacro %}