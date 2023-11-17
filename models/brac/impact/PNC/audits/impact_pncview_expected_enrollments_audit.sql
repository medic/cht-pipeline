{# in dbt Develop #}


{% set old_ancview_pregnancy %}
select
      uuid AS pregnancy_id,
      patient_id,
      patient_id AS patient_uuid,
      reported_by AS reported_by,
      reported_by_parent AS reported_by_parent,
      lmp::DATE AS lmp,
      reported AS reported
from dbt.ancview_pregnancy
{% endset %}


{% set new_impact_pncview_expected_enrollments %}
select
      pregnancy_id,
      patient_id,
      patient_uuid,
      reported_by,
      reported_by_parent,
      lmp,
      reported
from {{ ref("impact_pncview_expected_enrollments") }}
{% endset %}


{{ audit_helper.compare_queries(
    a_query=old_ancview_pregnancy,
    b_query=new_impact_pncview_expected_enrollments,
    primary_key="pregnancy_id"
) }}
