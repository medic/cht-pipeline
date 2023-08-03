#!/bin/bash
set -e
TESTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
set -o allexport
source ${TESTDIR}/.env
set +o allexport

export DBT_PROFILES_DIR=$PWD

echo Seeding test data ...
dbt seed --full-refresh
echo Running dbt ...
dbt run
echo Running tests ...
dbt test

