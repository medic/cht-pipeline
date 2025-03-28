# [2.0.0](https://github.com/medic/cht-pipeline/compare/v1.4.0...v2.0.0) (2025-03-28)


### Features

* **#190:** version 2: add multi-db, batching and tags ([#192](https://github.com/medic/cht-pipeline/issues/192)) ([d2067b6](https://github.com/medic/cht-pipeline/commit/d2067b6a6d64f7a2b970ce89f9a01599307d99b2)), closes [#190](https://github.com/medic/cht-pipeline/issues/190) [#172](https://github.com/medic/cht-pipeline/issues/172) [#190](https://github.com/medic/cht-pipeline/issues/190) [#156](https://github.com/medic/cht-pipeline/issues/156)


### BREAKING CHANGES

* **#190:** batching, instance and dbname added to document_metadata

# [1.4.0](https://github.com/medic/cht-pipeline/compare/v1.3.1...v1.4.0) (2024-12-06)


### Features

* add users meta base models ([#181](https://github.com/medic/cht-pipeline/issues/181)) ([c5b8285](https://github.com/medic/cht-pipeline/commit/c5b82855c24cd85b90af2b910d456b04650c4f49))

## [1.3.1](https://github.com/medic/cht-pipeline/compare/v1.3.0...v1.3.1) (2024-10-10)


### Bug Fixes

* **#176:** use python 3.12 ([#177](https://github.com/medic/cht-pipeline/issues/177)) ([d542e3c](https://github.com/medic/cht-pipeline/commit/d542e3c881bf62e3d168c7b340fa38056bfcafb2)), closes [#176](https://github.com/medic/cht-pipeline/issues/176)

# [1.3.0](https://github.com/medic/cht-pipeline/compare/v1.2.4...v1.3.0) (2024-09-26)


### Features

* remove date_of_birth column from person model ([76a0ddf](https://github.com/medic/cht-pipeline/commit/76a0ddfd5c213a7664647925a9ec9710d3663c0a))

## [1.2.4](https://github.com/medic/cht-pipeline/compare/v1.2.3...v1.2.4) (2024-09-25)


### Bug Fixes

* add extra validation to date format ([dfd3e7d](https://github.com/medic/cht-pipeline/commit/dfd3e7d17378cf1adfc5c71448e83e1d03cc8b86))

## [1.2.3](https://github.com/medic/cht-pipeline/compare/v1.2.2...v1.2.3) (2024-09-24)


### Bug Fixes

* **#165:** removing potentially ambiguous date format ([26f4da7](https://github.com/medic/cht-pipeline/commit/26f4da7157980b8e70fc5e412b68468a3e15daac)), closes [#165](https://github.com/medic/cht-pipeline/issues/165)

## [1.2.2](https://github.com/medic/cht-pipeline/compare/v1.2.1...v1.2.2) (2024-09-24)


### Bug Fixes

* remove unused macro ([3b2057f](https://github.com/medic/cht-pipeline/commit/3b2057ffd7a8c827dd29ce943afe6dbe0682906d))

## [1.2.1](https://github.com/medic/cht-pipeline/compare/v1.2.0...v1.2.1) (2024-09-17)


### Bug Fixes

* fail builds if dbt run fails ([7babb39](https://github.com/medic/cht-pipeline/commit/7babb396bc110889b14ff2d9f5ca0bea02eaff3f))

# [1.2.0](https://github.com/medic/cht-pipeline/compare/v1.1.1...v1.2.0) (2024-09-06)


### Features

* **#144:** add macros to reduce boilerplate ([#155](https://github.com/medic/cht-pipeline/issues/155)) ([ecf4487](https://github.com/medic/cht-pipeline/commit/ecf4487337b8380ada75fa404875bd6860cdfebc)), closes [#144](https://github.com/medic/cht-pipeline/issues/144) [#144](https://github.com/medic/cht-pipeline/issues/144) [#144](https://github.com/medic/cht-pipeline/issues/144) [#144](https://github.com/medic/cht-pipeline/issues/144) [#144](https://github.com/medic/cht-pipeline/issues/144) [#144](https://github.com/medic/cht-pipeline/issues/144) [#144](https://github.com/medic/cht-pipeline/issues/144) [#144](https://github.com/medic/cht-pipeline/issues/144)

## [1.1.1](https://github.com/medic/cht-pipeline/compare/v1.1.0...v1.1.1) (2024-09-04)


### Bug Fixes

* **#156:** unit test fixture not found when integrating with cht-sync ([#158](https://github.com/medic/cht-pipeline/issues/158)) ([ad88744](https://github.com/medic/cht-pipeline/commit/ad88744a4827824e290d474f4af8e82d64288e5f)), closes [#156](https://github.com/medic/cht-pipeline/issues/156)

# [1.1.0](https://github.com/medic/cht-pipeline/compare/v1.0.0...v1.1.0) (2024-09-02)


### Features

* update postgres version to 16 ([9c44962](https://github.com/medic/cht-pipeline/commit/9c44962cd6fcb130638c1de11535e34506ca4c3e))

# 1.0.0 (2024-08-27)


### Bug Fixes

* **#145:** docker compose instead of docker-compose ([b4ecea6](https://github.com/medic/cht-pipeline/commit/b4ecea6ac3abddd2d49e66dbd30a87409eae91dd)), closes [#145](https://github.com/medic/cht-pipeline/issues/145)


### Features

* **#148:** add automatic releases and versioning ([#152](https://github.com/medic/cht-pipeline/issues/152)) ([52cf12a](https://github.com/medic/cht-pipeline/commit/52cf12a50083a1f343a138cb4c5e1e5166644d7a)), closes [#148](https://github.com/medic/cht-pipeline/issues/148)
* **#72:** Replace hardcoded values with env variables ([#73](https://github.com/medic/cht-pipeline/issues/73)) ([3dfa8e7](https://github.com/medic/cht-pipeline/commit/3dfa8e710ae45531f999788f17124a78d69d46d0)), closes [#72](https://github.com/medic/cht-pipeline/issues/72)
