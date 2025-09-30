#!/bin/bash
set -e

# Airflow entrypoint with proper signal handling for graceful shutdown

# Global variable for main process PID
AIRFLOW_PID=""

# Function to wait for database
wait_for_db() {
    echo "Waiting for database to be ready..."
    timeout=60
    elapsed=0
    while ! pg_isready -h postgres -p 5432 -U bigdata_user 2>/dev/null; do
        if [ $elapsed -ge $timeout ]; then
            echo "Database connection timeout after ${timeout}s"
            return 1
        fi
        echo "Database is not ready, waiting... (${elapsed}s/${timeout}s)"
        sleep 2
        elapsed=$((elapsed + 2))
    done
    echo "Database is ready!"
    return 0
}

# Function to handle shutdown signals
shutdown() {
    echo "Received shutdown signal, initiating graceful shutdown..."

    if [ ! -z "$AIRFLOW_PID" ] && kill -0 "$AIRFLOW_PID" 2>/dev/null; then
        echo "Stopping Airflow processes (PID: $AIRFLOW_PID)..."

        # Send SIGTERM to allow graceful shutdown
        kill -TERM "$AIRFLOW_PID" 2>/dev/null || true

        # Wait for up to 30 seconds for graceful shutdown
        for i in {1..30}; do
            if ! kill -0 "$AIRFLOW_PID" 2>/dev/null; then
                echo "Airflow stopped gracefully"
                exit 0
            fi
            sleep 1
        done

        # Force kill if still running
        echo "Forcing Airflow shutdown..."
        kill -KILL "$AIRFLOW_PID" 2>/dev/null || true
    fi

    exit 0
}

# Trap SIGTERM and SIGINT signals
trap shutdown SIGTERM SIGINT

# Main execution
if [ "$1" = "standalone" ]; then
    wait_for_db || {
        echo "Failed to connect to database, exiting..."
        exit 1
    }

    echo "Starting Airflow standalone mode..."

    # Start Airflow standalone in background
    airflow standalone &
    AIRFLOW_PID=$!

    echo "Airflow started with PID: $AIRFLOW_PID"

    # Wait for the airflow process
    wait "$AIRFLOW_PID" 2>/dev/null || true
    EXIT_CODE=$?

    echo "Airflow exited with code: $EXIT_CODE"
    exit $EXIT_CODE

elif [ "$1" = "webserver" ]; then
    wait_for_db || exit 1
    echo "Starting Airflow webserver..."
    exec airflow webserver

elif [ "$1" = "scheduler" ]; then
    wait_for_db || exit 1
    echo "Starting Airflow scheduler..."
    exec airflow scheduler

else
    # For any other command, just execute it
    exec "$@"
fi