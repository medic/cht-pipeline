#!/bin/bash
set -e
export POSTGRES_USER=root
export POSTGRES_PASSWORD=supercoolpassword
export POSTGRES_DB=data
export POSTGRES_TABLE=couchdb
export POSTGRES_SCHEMA=v1
export DBT_POSTGRES_USER=dbt_user
export DBT_POSTGRES_PASSWORD=supercoolpassword
export DBT_POSTGRES_SCHEMA=dbt
export DBT_POSTGRES_HOST=postgres
export ROOT_POSTGRES_SCHEMA=v1

echo Creating database $POSTGRES_DB
docker run --name pgtest -p 5432:5432 -e POSTGRES_USER=$POSTGRES_USER -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD -e POSTGRES_DB=$POSTGRES_DB -d postgres:13

sleep 10

export PGPASSWORD=$POSTGRES_PASSWORD

## DO NOT put any additional SQL here
#
#  Put all SQL into DBT. Bootstrapping should be the absolute minimum
#
echo Setting up defaults

psql -h localhost -p 5432 -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER $DBT_POSTGRES_USER WITH PASSWORD '$DBT_POSTGRES_PASSWORD';
    CREATE SCHEMA IF NOT EXISTS $DBT_POSTGRES_SCHEMA AUTHORIZATION $DBT_POSTGRES_USER;
EOSQL
