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
cd ..
echo Install dbt dependencies ...
dbt deps
echo Seeding test data ...
dbt seed --full-refresh
echo Running dbt ...
dbt run
echo Running tests ...
dbt test

