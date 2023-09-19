#!/bin/bash
set -e
export POSTGRES_DB=data
export POSTGRES_TABLE=couchdb
export DBT_POSTGRES_USER=dbt_user
export DBT_POSTGRES_PASSWORD=supercoolpassword
export DBT_POSTGRES_SCHEMA=dbt
export DBT_POSTGRES_HOST=postgres
export ROOT_POSTGRES_SCHEMA=v1

export DBT_PROFILES_DIR=$PWD
echo Install dbt dependencies ...
dbt deps
echo Seeding test data ...
dbt seed --full-refresh
echo Running dbt ...
dbt run --exclude indexes get_dashboard_data_hh_impact+ get_dashboard_data_pnc_impact+ useview_assessment+ useview_assessment_follow_up+ useview_family_survey+ useview_health_forum+ useview_household_survey+ useview_patient_record+ useview_postnatal_care+ useview_pregnancy_visit+ useview_visit+ immunization_followup+ contactview_person_fields+ impact_ancview_pregnancy+ immunization_followup+
echo Running tests ...
dbt test
#echo Generating documentation...
#dbt docs generate
#dbt docs serve

