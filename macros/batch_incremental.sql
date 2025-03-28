-- for incremental runs
-- assume all in the source table with timestamp <= the max timestamp in the target table
-- have already been inserted
-- select up to batch_size rows from the source table where timestamp > max_timestamp, 
-- ordered by timestamp
-- then union with all all rows in the source table that have the same timestamp
-- as the maximum timestamp in the btach to be inserted
-- to ensure that all rows with the same timestamp are inserted in the same batch
{% macro batch_incremental(source_cte_name) %}
    --define a CTE current_batch with a limit of batch_size
    , current_batch AS (
      SELECT * FROM {{ source_cte_name }}

			{% if is_incremental() %}
      WHERE saved_timestamp > {{ max_existing_timestamp('saved_timestamp') }}
			{% endif %}

      {% if var("batch_size", none) is not none %}
        ORDER BY saved_timestamp
        LIMIT {{ var('batch_size') }}
      {% endif %}
    )

    -- union the CTE with a query getting all records with highest timestamp in current_batch
    SELECT * FROM {{ source_cte_name }}
    WHERE saved_timestamp = (SELECT MAX(saved_timestamp) FROM current_batch)
    UNION
    SELECT * FROM current_batch

{% endmacro %}
