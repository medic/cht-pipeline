export DBT_PROFILES_DIR=$PWD
echo Install dbt dependencies ...
dbt deps
echo Running dbt ...
dbt run
run_exit_status=$?
if [ $run_exit_status -ne 0 ]; then
  echo "DBT run failed"
  exit $run_exit_status
fi
echo Running tests ...
dbt test
