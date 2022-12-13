CHT Pipeline
========================

CHT Pipeline is a tool used to define data models for transforming the raw data we get from Couch DB into models that can then be queried to build dashboards

# Installation
This project canbe run in one of two ways:
1. Using the docker image and passed as an argument to [cht-sync](https://github.com/medic/cht-sync)
2. Using the local installation of DBT

## Docker
1. Follow the instructions in [cht-sync](https://github.com/medic/cht-sync) to set up the required environment variables
1. Add the path to your pipeline branch to the [docker-compose.postgres.yml](https://github.com/medic/cht-sync/blob/main/docker-compose.postgres.yml#L13) file in the cht-sync project
1. Run `docker-compose up` to run the pipeline

## Local DBT
1. Follow the [DBT] (https://docs.getdbt.com/docs/get-started/installation) installation instructions
1. Run `dbt deps` to install the required packages
1. Run `dbt run` to run the pipeline
