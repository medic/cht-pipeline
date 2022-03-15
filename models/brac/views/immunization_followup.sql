SELECT DISTINCT ON (patient_id)
				"inputs/source_id",
				vaccines_administered 
			FROM formview_immunization_follow_up
			WHERE 
			(date_trunc('month', reported::timestamp)::date) <= (date_trunc('MONTH',('{{ var("end_date") }}')::date)::date) 
			ORDER BY patient_id, reported DESC