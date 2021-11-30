#!/bin/bash

while true
do
    export CHT_PIPELINE_SUBPACKAGE=$(curl $CHT_PIPELINE_CONFIG | jq '.subpackage.url' )
    export CHT_PIPELINE_SUBPACKAGE_VERSION=$(curl $CHT_PIPELINE_CONFIG | jq '.subpackage.version' )
    dbt run --profiles-dir .dbt --profile medic
    sleep 5s
done