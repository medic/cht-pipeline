#!/bin/bash
set -e
TESTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
set -o allexport
source ${TESTDIR}/.env
set +o allexport

echo Creating database $POSTGRES_DB
docker run --name pgdev -p 5432:5432 -e POSTGRES_USER=$POSTGRES_USER -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD -e POSTGRES_DB=$POSTGRES_DB -d postgres:13

sleep 10

echo Creating table $POSTGRES_TABLE

export PGPASSWORD=$POSTGRES_PASSWORD

## DO NOT put any additional SQL here
#
#  Put all SQL into DBT. Bootstrapping should be the absolute minimum
#
psql -h localhost -p 5432 -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS $POSTGRES_SCHEMA AUTHORIZATION $POSTGRES_USER;
    CREATE TABLE IF NOT EXISTS $POSTGRES_SCHEMA.$POSTGRES_TABLE (
        "@version" TEXT,
        "@timestamp" TIMESTAMP,
        "_id" TEXT,
        "_rev" TEXT,
        doc jsonb,
        doc_as_upsert BOOLEAN,
        UNIQUE ("_id", "_rev")
    );
EOSQL

echo Setting up defaults

psql -h localhost -p 5432 -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER $DBT_POSTGRES_USER WITH PASSWORD '$DBT_POSTGRES_PASSWORD';
    CREATE SCHEMA IF NOT EXISTS $DBT_POSTGRES_SCHEMA AUTHORIZATION $DBT_POSTGRES_USER;

    GRANT USAGE ON SCHEMA $POSTGRES_SCHEMA TO $DBT_POSTGRES_USER;
    GRANT SELECT ON ALL TABLES IN SCHEMA $POSTGRES_SCHEMA TO $DBT_POSTGRES_USER;
    GRANT SELECT ON ALL SEQUENCES IN SCHEMA $POSTGRES_SCHEMA TO $DBT_POSTGRES_USER;
    GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA $POSTGRES_SCHEMA TO $DBT_POSTGRES_USER;

EOSQL

