docker compose up -d postgres

# Give PostgreSQL some time to start up
echo "Waiting for PostgreSQL to be ready..."
sleep 10

docker compose up --build --abort-on-container-exit dbt
exit_status=$?

# Stop and remove all containers
docker compose down

# Determine if the tests passed or failed based on the exit status
if [ $exit_status -eq 0 ]; then
  echo "DBT tests passed"
  exit 0
else
  echo "DBT tests failed"
  exit 1
fi
