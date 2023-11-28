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
echo Check test coverage ...
dbt run-operation required_tests

