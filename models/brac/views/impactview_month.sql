SELECT 
	 	generate_series(date_trunc('month', now() - '1 year'::interval), now(), '1 mon'::interval)::date AS month,
	    date_part('epoch', generate_series(date_trunc('month', now() - '1 year'::interval), now(), '1 mon'::interval)::date) AS epoch
	ORDER BY
		epoch