{%- set form_indexes = [
  {'columns': ['test']}]
-%}
{% set form_columns %}
  NULL as test
{% endset %}
{{ cht_form_model('pregnancy_danger_sign', form_columns, form_indexes) }}

