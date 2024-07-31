FROM python:3 AS testing

RUN pip install flake8

RUN flake8 dbt-run.py

FROM python:3 AS release

RUN pip install --upgrade cffi \
    && pip install cryptography~=3.4 \
    && pip install dbt-core dbt-postgres

RUN adduser dbt
USER dbt

WORKDIR /dbt/
COPY test/profiles.yml profiles.yml 
COPY dbt_project.yml dbt_project.yml
COPY models models
COPY macros macros
COPY test/sqltest tests
COPY test/run_dbt_tests_docker.sh run_dbt_tests_docker.sh

CMD ["bash", "./run_dbt_tests_docker.sh"]
