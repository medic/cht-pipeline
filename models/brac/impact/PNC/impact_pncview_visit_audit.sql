{# in dbt Develop #}


{% set old_useview_postnatal_care %}
select
      uuid,
      patient_id,
      form
from dbt.useview_postnatal_care
{% endset %}


{% set new_impact_pncview_visit %}
select
      uuid,
      patient_id,
      form
from {{ ref("impact_pncview_visit") }}
{% endset %}


{{ audit_helper.compare_queries(
    a_query=old_useview_postnatal_care,
    b_query=new_impact_pncview_visit,
    primary_key="uuid"
) }}
