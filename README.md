A set of SQL queries that transform raw CouchDB data into a more useful format. It uses `dbt` to define the models that are translated into PostgreSQL tables or views, which makes it easier to query the data in the analytics platform of choice.

## Local Setup
Follow the instructions in [the Local CHT Sync Setup documentation](https://docs.communityhealthtoolkit.org/apps/guides/data/analytics/setup/) to set up CHT Sync locally.

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

## Release Process
This repo has an automated release process where each feature/bug fix will be released immediately after it is merged to `main`. The release type is determined by the commit message format. Have a look at the development workflow in the [Contributor Handbook](https://docs.communityhealthtoolkit.org/contribute/code/workflow/) for more information.

### Commit message format

The commit format should follow the convention outlined in the [CHT docs](https://docs.communityhealthtoolkit.org/contribute/code/workflow/#commit-message-format).
Examples are provided below.

| Type        | Example commit message                                                                              | Release type |
|-------------|-----------------------------------------------------------------------------------------------------|--------------|
| Bug fixes   | fix(#123): rename column names                                                                      | patch        |
| Performance | perf(#789): add new indexes                                                                         | patch        |
| Features    | feat(#456): add new model                                                                           | minor        |
| Non-code    | chore(#123): update README                                                                          | none         |
| Breaking    | perf(#2): remove data_record model <br/> BREAKING CHANGE: form models should now read from new_model| major        |
