# CHT Pipeline

CHT Pipeline is a set of SQL queries that transform raw CouchDB data into a more useful format. It uses `dbt` to define the models that are translated into PostgreSQL tables or views, which makes it easier to query the data in the analytics platform of choice.

## Local Setup
Follow the instructions in [the Local CHT Sync Setup documentation](https://docs.communityhealthtoolkit.org/apps/guides/data/analytics/setup/) to set up CHT Sync with CHT Pipeline locally.

## Run dbt models unit tests locally

### Prerequisites
- `Docker`

### Run the tests

1. Navigate to `tests` folder.
2. Run the test script

```sh
# set environment variables, install dbt dependencies, seed data, run dbt, run test
./run_dbt_tests.sh
```
