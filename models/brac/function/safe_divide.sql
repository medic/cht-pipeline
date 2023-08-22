{{ config(materialized = 'raw_sql') }}  

CREATE OR REPLACE FUNCTION {{this}}(numerator NUMERIC, denominator NUMERIC, round_dec INT DEFAULT 2) RETURNS FLOAT AS $$
    SELECT
    	CASE
    		WHEN denominator = 0 THEN 0
    		ELSE round(100*(numerator / (denominator)::float)::numeric, round_dec)::float
    	END;
$$ LANGUAGE SQL IMMUTABLE;