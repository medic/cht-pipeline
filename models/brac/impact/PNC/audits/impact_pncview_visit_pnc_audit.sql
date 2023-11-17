{# in dbt Develop #}


{% set old_useview_postnatal_care %}
select
      uuid,
      patient_id,
      form,
      reported_by,
      reported_by_parent,
      reported::DATE AS pnc_visit_date,
      follow_up_count AS pnc_visit_number,
      reported,
      baby_danger_signs <> '' AS visit_with_danger_sign
from dbt.useview_postnatal_care
{% endset %}


{% set new_impact_pncview_visit %}
select
      uuid,
      patient_id,
      form,
      reported_by,
      reported_by_parent,
      pnc_visit_date,
      pnc_visit_number,
      reported,
      visit_with_danger_sign
from {{ ref("impact_pncview_visit") }}
{% endset %}


{{ audit_helper.compare_queries(
    a_query=old_useview_postnatal_care,
    b_query=new_impact_pncview_visit,
    primary_key="uuid"
) }}
