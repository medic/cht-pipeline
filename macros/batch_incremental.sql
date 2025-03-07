-- for incremental runs
-- select ALL records from source where document_metadata.saved_timestamp
-- is exactly equal to source.saved_timestamp
-- then union this with at most batch_size records
-- where document_metadata.source_timestamp is strictly greater than
-- document_metadata.saved_timestamp
-- this ensures that all records with the same timestamp are inserted
-- but limits later records to batch_size
{% macro batch_incremental(source_cte_name) %}
  {% if is_incremental() %}
    --define a CTE current_batch with a limit of batch_size
    , current_batch AS (
      SELECT * FROM {{ source_cte_name }}
      WHERE saved_timestamp > {{ max_existing_timestamp('saved_timestamp') }}
      {% if var("batch_size", none) is not none %}
        ORDER BY saved_timestamp
        LIMIT {{ var('batch_size') }}
      {% endif %}
    )

    -- union the CTE with a query getting all records with equal timestamp
    SELECT * FROM {{ source_cte_name }}
    WHERE saved_timestamp = {{ max_existing_timestamp('saved_timestamp') }}
    UNION ALL
    SELECT * FROM current_batch

  -- if not incremental (the table is being created for the first time)
  -- and batch size is defined
  -- apply a limit of batch size so the entire table is not created
  -- in a single batch
  {% else %}
    SELECT * FROM {{ source_cte_name }}
    {% if var("batch_size", none) is not none %}
      ORDER BY saved_timestamp
      LIMIT {{ var('batch_size') }}
    {% endif %}
  {% endif %}
{% endmacro %}
