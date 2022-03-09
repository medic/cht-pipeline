SELECT 
			chp.area_uuid,
			date_trunc('MONTH', reported) AS reported_month,
			COUNT(DISTINCT patient_id) FILTER(WHERE preg_test != 'neg') AS count
		FROM {{ ref("useview_pregnancy") }} AS preg
		LEFT JOIN {{ ref("contactview_ch") }} AS chp ON chp.uuid =  preg.chw 
		WHERE date_trunc('month',reported) ::DATE <= date_trunc('MONTH',end_date)::DATE
		GROUP BY 
			area_uuid, 
			reported_month