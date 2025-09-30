#!/bin/bash
set -e

# Create airflow_db database for Apache Airflow
# This script runs before init.sql

echo "Creating airflow_db database..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE airflow_db'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db')\gexec
EOSQL

echo "airflow_db database created successfully"