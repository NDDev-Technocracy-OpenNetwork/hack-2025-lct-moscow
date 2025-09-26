#!/bin/bash
set -e

# Function to wait for database
wait_for_db() {
    echo "Waiting for database to be ready..."
    while ! pg_isready -h postgres -p 5432 -U bigdata_user; do
        echo "Database is not ready, waiting..."
        sleep 2
    done
    echo "Database is ready!"
}

# Initialize Airflow database if needed
init_airflow() {
    echo "Initializing Airflow database..."

    # Initialize database
    airflow db init

    # Create connections
    airflow connections add bigdata_postgres \
        --conn-type postgres \
        --conn-host postgres \
        --conn-port 5432 \
        --conn-login bigdata_user \
        --conn-password bigdata_pass \
        --conn-schema bigdata_db \
        || echo "Connection bigdata_postgres already exists"

    # Create admin user
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@bigdata.com \
        --password admin123 \
        || echo "Admin user already exists"

    echo "Airflow initialization completed!"
}

# Main execution
if [ "$1" = "webserver" ]; then
    wait_for_db
    init_airflow
    echo "Starting Airflow webserver..."
    exec airflow webserver
elif [ "$1" = "scheduler" ]; then
    wait_for_db
    echo "Starting Airflow scheduler..."
    exec airflow scheduler
else
    # For any other command, just execute it
    exec "$@"
fi