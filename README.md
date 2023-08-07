CHT Pipeline
========================

CHT Pipeline is a tool used to define data models for transforming the raw data we get from Couch DB into models that can then be queried to build dashboards

# Dev installation instructions

1. Follow the instructions in [cht-sync](https://github.com/medic/cht-sync) to set up the required environment variables
1. Run `docker-compose up` in the cht-sync directory to run the pipeline. Please ensure the `DATAEMON_INITAL_PACKAGE` env variable is set to your preferred branch of the cht-pipeline repo.

## Run unit test locally
### Prerequisites

- `Docker`
- `PostgreSQL client`
- `dbt-postgres`
- `Python`

1. Navigate to tests folder
2. Run setup script:

```sh
# Set environment variables, create postgres database, schema and user:
./setup.sh
```

3. Run dbt test

```sh
# set environment variables, install dbt dependencies, seed data, run dbt, run test
./run_dbt_tests.sh
```

4. Clean up:
```sh
./tear_down.sh
```
