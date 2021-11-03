#!/bin/bash
set -e

for POSTGRES_TABLE in $POSTGRES_TABLES
do

echo Creating $POSTGRES_TABLE

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE public.$POSTGRES_TABLE (
        "@version" TEXT,
        "@timestamp" TIMESTAMP,
        "_id" TEXT,
        "_rev" TEXT,
        doc jsonb,
        doc_as_upsert BOOLEAN,
        UNIQUE ("_id", "_rev")
    );
EOSQL

done