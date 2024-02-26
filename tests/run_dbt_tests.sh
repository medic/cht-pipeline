#!/bin/bash
set -e
export POSTGRES_USER=root
export POSTGRES_PASSWORD=supercoolpassword
export POSTGRES_DB=data
export POSTGRES_SCHEMA=v1
export POSTGRES_TABLE=medic
export DBT_POSTGRES_USER=dbt_user
export DBT_POSTGRES_PASSWORD=supercoolpassword
export DBT_POSTGRES_SCHEMA=dbt
export DBT_POSTGRES_HOST=postgres

export DBT_PROFILES_DIR=$PWD
echo Install dbt dependencies ...
dbt deps
echo Running dbt ...
dbt run
echo Running tests ...
dbt test

