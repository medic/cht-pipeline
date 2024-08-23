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

## Release Process
This repo has an automated release process where each feature/bug fix will be released immediately after it is merged to `main`:

1. Update QA with the work to be done to ensure they're informed and can guide development.
2. Create a ticket for the feature/bug fix.
3. Submit a PR, and make sure that the PR title is clear, readable, and follows the strict commit message format described in the commit message format section below. If the PR title does not comply, automatic release will fail.
4. Have the PR reviewed.
5. Squash and merge the PR to `main`. The commit message should be the already-formatted PR title but double check that it's clear, readable, and follows the strict commit message format to make sure the automatic release works as expected.
6. Close the ticket.

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
