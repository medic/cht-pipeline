WITH facilities_CTE AS
	(
		SELECT
			uuid AS facility_join_field,
			name AS facility_name
		FROM
			contactview_metadata
		WHERE
			type = 'district_hospital'
			AND name <> 'HQ'
			AND name <> 'HQ OVC'
			
	)
	
	SELECT 
		month,
		epoch,
		facility_join_field,
		facility_name
		
	FROM
		impactview_month
		CROSS JOIN facilities_CTE
	
	ORDER BY 
		epoch,
		facility_name