{% test assert_date_format(model, column_name) %}
  SELECT *
  FROM {{ model }}
  WHERE {{ column_name }} IS NOT NULL
    AND (
      NOT (CAST({{ column_name }} AS TEXT) ~ '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$')
      OR NOT (
        TO_DATE(CAST({{ column_name }} AS TEXT), 'YYYY-MM-DD') = CAST({{ column_name }} AS TEXT)
        AND (
          (EXTRACT(MONTH FROM TO_DATE(CAST({{ column_name }} AS TEXT), 'YYYY-MM-DD')) != 2)
          OR (EXTRACT(DAY FROM TO_DATE(CAST({{ column_name }} AS TEXT), 'YYYY-MM-DD')) <=
              CASE
                WHEN EXTRACT(YEAR FROM TO_DATE(CAST({{ column_name }} AS TEXT), 'YYYY-MM-DD')) % 4 = 0
                  AND (EXTRACT(YEAR FROM TO_DATE(CAST({{ column_name }} AS TEXT), 'YYYY-MM-DD')) % 100 != 0
                  OR EXTRACT(YEAR FROM TO_DATE(CAST({{ column_name }} AS TEXT), 'YYYY-MM-DD')) % 400 = 0) THEN 29
                ELSE 28
              END
          )
        )
        AND NOT (
          (EXTRACT(MONTH FROM TO_DATE(CAST({{ column_name }} AS TEXT), 'YYYY-MM-DD')) IN (4, 6, 9, 11))
          AND EXTRACT(DAY FROM TO_DATE(CAST({{ column_name }} AS TEXT), 'YYYY-MM-DD')) > 30
        )
      )
    )
{% endtest %}
