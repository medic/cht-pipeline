FROM python:3 AS testing

RUN pip install flake8 \
  && flake8 dbt-run.py

FROM python:3 AS release

RUN pip install --upgrade cffi \
  && pip install cryptography~=3.4 \
  && pip install dbt-core dbt-postgres \
  && adduser dbt
USER dbt

WORKDIR /dbt/
COPY tests/profiles.yml profiles.yml
COPY dbt_project.yml dbt_project.yml
COPY packages.yml packages.yml
COPY models models
COPY macros macros
COPY tests/fixtures tests/fixtures
COPY tests/sqltest tests
COPY tests/run_dbt_tests_docker.sh run_dbt_tests_docker.sh

CMD ["bash", "./run_dbt_tests_docker.sh"]
