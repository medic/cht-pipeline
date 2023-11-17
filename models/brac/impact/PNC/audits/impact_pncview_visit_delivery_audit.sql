{# in dbt Develop #}


{% set old_pncview_actual_enrollments %}
select
      delivery_id as uuid,
      patient_id,
      facility_delivery,
      delivery_date
from dbt.pncview_actual_enrollments
{% endset %}


{% set new_impact_pncview_visit %}
select
      uuid,
      patient_id,
      facility_delivery,
      delivery_date
from {{ ref("impact_pncview_visit") }}
{% endset %}


{{ audit_helper.compare_queries(
    a_query=old_pncview_actual_enrollments,
    b_query=new_impact_pncview_visit,
    primary_key="uuid"
) }}
