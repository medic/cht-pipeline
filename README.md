# CHT Pipeline

CHT Pipeline is a tool used to define `dbt` data models for transforming the raw data we get from CouchDB into models that can then be queried to build dashboards.

## Local Setup
Follow the instructions in [the Local CHT Sync Setup documentation](https://docs.communityhealthtoolkit.org/apps/guides/data/analytics/setup/) to set up CHT Sync with CHT Pipeline locally.

## Run dbt models unit tests locally

1. Navigate to `tests` folder.
2. Run the `setup` script:

```sh
# Set environment variables, create postgres database, schema and user:
./setup.sh
```

3. Run the `dbt` tests:

```sh
# set environment variables, install dbt dependencies, seed data, run dbt, run test
./run_dbt_tests.sh
```

4. Check for test coverage:

```sh
# Run dbt run-operation required_tests command
./check_tests_coverage.sh
```

5. Clean up:
```sh
./tear_down.sh
```
