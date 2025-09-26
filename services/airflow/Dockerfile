# ================================================================================================
# BIGDATA PROCESSING SYSTEM - AIRFLOW DOCKERFILE (PRODUCTION)
# ================================================================================================
# Production-optimized Apache Airflow with dlt integration and security hardening
# Based on official Airflow image with additional production configurations
# ================================================================================================

FROM apache/airflow:2.7.3-python3.11

# Switch to root for system installations
USER root

# Install system dependencies with security updates
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    gcc \
    g++ \
    postgresql-client \
    redis-tools \
    libxml2-dev \
    libxslt-dev \
    libpq-dev \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create additional directories for configuration and logs
RUN mkdir -p \
    /opt/airflow/config \
    /opt/airflow/plugins/custom_operators \
    /opt/airflow/plugins/custom_hooks \
    /opt/airflow/logs/scheduler \
    /opt/airflow/logs/webserver \
    && chown -R airflow:root /opt/airflow

# Switch back to airflow user
USER airflow

# Copy requirements first for better caching
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies with security updates
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Create Airflow configuration with production settings
RUN cat > /opt/airflow/airflow.cfg << 'EOF'
[core]
dags_folder = /opt/airflow/dags
hostname_callable = airflow.utils.net.get_hostname
default_timezone = utc
executor = LocalExecutor
parallelism = 32
max_active_runs_per_dag = 16
dags_are_paused_at_creation = False
max_active_tasks_per_dag = 16
load_examples = False
plugins_folder = /opt/airflow/plugins
donot_pickle = True
dagbag_import_timeout = 30.0
dagbag_import_error_tracebacks = True
dagbag_import_error_traceback_depth = 2
dag_file_processor_timeout = 50

[logging]
logging_level = INFO
fab_logging_level = WARN
colored_console_log = False
colored_formatter_class = airflow.utils.log.colored_log.CustomTTYColoredFormatter
colored_log_format = [%%(blue)s%%(asctime)s%%(reset)s] {{%%(blue)s%%(filename)s:%%(reset)s%%(lineno)d}} %%(log_color)s%%(levelname)s%%(reset)s - %%(log_color)s%%(message)s%%(reset)s
log_format = [%%(asctime)s] {{%%(filename)s:%%(lineno)d}} %%(levelname)s - %%(message)s
simple_log_format = %%(asctime)s %%(levelname)s - %%(message)s
log_filename_template = {{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log
log_processor_filename_template = {{ filename }}.log

[webserver]
base_url = http://localhost:8080
default_ui_timezone = UTC
expose_config = True
web_server_port = 8080
web_server_host = 0.0.0.0
secret_key = temporary_key_change_in_production
workers = 4
worker_refresh_batch_size = 1
worker_refresh_interval = 30
reload_on_plugin_change = False
session_lifetime_minutes = 43200

[email]
email_backend = airflow.utils.email.send_email_smtp

[scheduler]
catchup_by_default = False
max_tis_per_query = 512
schedule_after_task_execution = True
parsing_processes = 2
scheduler_zombie_task_threshold = 300

[celery]
worker_concurrency = 16

[operators]
default_owner = airflow

[hive]
default_hive_mapred_queue = default

[admin]
hide_sensitive_var_fields = True
EOF

# Copy DAGs, plugins, and configuration with proper ownership
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root plugins/ /opt/airflow/plugins/
COPY --chown=airflow:root config/ /opt/airflow/config/

# Create production entrypoint script
RUN cat > /opt/airflow/entrypoint.sh << 'EOF'
#!/bin/bash
set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}Starting BigData Processing Airflow...${NC}"

# Logging functions
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

error_log() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}" >&2
}

warn_log() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

info_log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')] INFO: $1${NC}"
}

# Function to validate required environment variables
validate_environment() {
    log "Validating environment variables..."

    local required_vars=(
        "POSTGRES_USER"
        "POSTGRES_PASSWORD"
        "POSTGRES_DB"
        "AIRFLOW_ADMIN_USERNAME"
        "AIRFLOW_ADMIN_PASSWORD"
        "AIRFLOW_ADMIN_EMAIL"
    )

    local missing_vars=()

    for var in "${required_vars[@]}"; do
        if [[ -z "${!var}" ]]; then
            missing_vars+=("$var")
        fi
    done

    if [[ ${#missing_vars[@]} -gt 0 ]]; then
        error_log "Missing required environment variables: ${missing_vars[*]}"
        exit 1
    fi

    log "Environment validation successful"
}

# Function to wait for database with timeout
wait_for_database() {
    log "Waiting for PostgreSQL database to be ready..."

    local timeout=60
    local counter=0

    while ! pg_isready -h postgres -p 5432 -U "${POSTGRES_USER}" >/dev/null 2>&1; do
        if [[ $counter -ge $timeout ]]; then
            error_log "Database connection timeout after ${timeout} seconds"
            exit 1
        fi

        info_log "Database not ready, waiting... (${counter}/${timeout})"
        sleep 2
        ((counter+=2))
    done

    log "PostgreSQL database is ready!"
}

# Function to test additional service connectivity
test_service_connectivity() {
    log "Testing connectivity to other services..."

    # Test Redis if configured
    if [[ -n "${REDIS_URL}" ]]; then
        if redis-cli -h redis -p 6379 ping >/dev/null 2>&1; then
            info_log "Redis connectivity: OK"
        else
            warn_log "Redis connectivity: FAILED (continuing anyway)"
        fi
    fi

    # Test ClickHouse if configured
    if [[ -n "${CLICKHOUSE_URL}" ]]; then
        if curl -s "${CLICKHOUSE_URL}/ping" | grep -q "Ok" >/dev/null 2>&1; then
            info_log "ClickHouse connectivity: OK"
        else
            warn_log "ClickHouse connectivity: FAILED (continuing anyway)"
        fi
    fi
}

# Function to initialize Airflow database and create admin user
initialize_airflow() {
    log "Initializing Airflow..."

    # Initialize database
    info_log "Initializing Airflow database schema..."
    airflow db init || {
        error_log "Failed to initialize Airflow database"
        exit 1
    }

    # Create admin user
    info_log "Creating admin user..."
    airflow users create \
        --username "${AIRFLOW_ADMIN_USERNAME}" \
        --firstname "${AIRFLOW_ADMIN_FIRSTNAME:-Admin}" \
        --lastname "${AIRFLOW_ADMIN_LASTNAME:-User}" \
        --role "${AIRFLOW_ADMIN_ROLE:-Admin}" \
        --email "${AIRFLOW_ADMIN_EMAIL}" \
        --password "${AIRFLOW_ADMIN_PASSWORD}" \
        || info_log "Admin user already exists or creation failed"

    # Create database connections
    info_log "Creating database connections..."

    # PostgreSQL connection
    airflow connections add bigdata_postgres \
        --conn-type postgres \
        --conn-host postgres \
        --conn-port 5432 \
        --conn-login "${POSTGRES_USER}" \
        --conn-password "${POSTGRES_PASSWORD}" \
        --conn-schema "${POSTGRES_DB}" \
        || info_log "PostgreSQL connection already exists"

    # ClickHouse connection (if configured)
    if [[ -n "${CLICKHOUSE_USER}" && -n "${CLICKHOUSE_PASSWORD}" ]]; then
        airflow connections add bigdata_clickhouse \
            --conn-type http \
            --conn-host clickhouse \
            --conn-port 8123 \
            --conn-login "${CLICKHOUSE_USER}" \
            --conn-password "${CLICKHOUSE_PASSWORD}" \
            --conn-extra '{"database": "analytics"}' \
            || info_log "ClickHouse connection already exists"
    fi

    # Redis connection (if configured)
    if [[ -n "${REDIS_URL}" ]]; then
        airflow connections add bigdata_redis \
            --conn-type redis \
            --conn-host redis \
            --conn-port 6379 \
            || info_log "Redis connection already exists"
    fi

    log "Airflow initialization completed!"
}

# Function to run database upgrade
upgrade_database() {
    log "Running database upgrade..."
    airflow db upgrade || {
        error_log "Database upgrade failed"
        exit 1
    }
}

# Main execution logic
main() {
    validate_environment
    wait_for_database
    test_service_connectivity

    case "$1" in
        webserver)
            info_log "Starting Airflow webserver..."
            initialize_airflow

            # Set production configuration
            export AIRFLOW__WEBSERVER__SECRET_KEY="${AIRFLOW_WEBSERVER_SECRET_KEY}"
            export AIRFLOW__WEBSERVER__EXPOSE_CONFIG="${AIRFLOW_EXPOSE_CONFIG:-true}"

            exec airflow webserver
            ;;
        scheduler)
            info_log "Starting Airflow scheduler..."
            upgrade_database

            # Set scheduler-specific configuration
            export AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT=False
            export AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG="${AIRFLOW_MAX_ACTIVE_RUNS_PER_DAG:-1}"

            exec airflow scheduler
            ;;
        worker)
            info_log "Starting Airflow worker..."
            upgrade_database
            exec airflow celery worker
            ;;
        flower)
            info_log "Starting Airflow flower..."
            exec airflow celery flower
            ;;
        version)
            airflow version
            ;;
        *)
            info_log "Executing custom command: $*"
            exec "$@"
            ;;
    esac
}

# Run main function with all arguments
main "$@"
EOF

# Create health check script for Airflow
RUN cat > /opt/airflow/healthcheck.py << 'EOF'
#!/usr/bin/env python3
"""
Health check script for Airflow services
"""
import sys
import requests
import json
from datetime import datetime
import subprocess
import os

def check_webserver_health():
    """Check if Airflow webserver is healthy"""
    try:
        response = requests.get('http://localhost:8080/health', timeout=10)
        if response.status_code == 200:
            return True, "Webserver healthy"
        return False, f"Webserver returned {response.status_code}"
    except Exception as e:
        return False, f"Webserver check failed: {e}"

def check_scheduler_health():
    """Check if Airflow scheduler is healthy"""
    try:
        result = subprocess.run([
            'airflow', 'jobs', 'check', '--job-type', 'SchedulerJob'
        ], capture_output=True, text=True, timeout=10)

        if result.returncode == 0:
            return True, "Scheduler healthy"
        return False, f"Scheduler check failed: {result.stderr}"
    except Exception as e:
        return False, f"Scheduler check failed: {e}"

def check_database_connectivity():
    """Check database connectivity"""
    try:
        result = subprocess.run([
            'airflow', 'db', 'check'
        ], capture_output=True, text=True, timeout=10)

        if result.returncode == 0:
            return True, "Database connectivity OK"
        return False, f"Database check failed: {result.stderr}"
    except Exception as e:
        return False, f"Database check failed: {e}"

def main():
    service_type = os.getenv('AIRFLOW_SERVICE_TYPE', 'webserver')

    checks = {
        'timestamp': datetime.utcnow().isoformat(),
        'service_type': service_type,
        'database': check_database_connectivity(),
    }

    if service_type == 'webserver':
        checks['webserver'] = check_webserver_health()
    elif service_type == 'scheduler':
        checks['scheduler'] = check_scheduler_health()

    all_passed = all(check[0] for check in checks.values() if isinstance(check, tuple))

    if all_passed:
        print(json.dumps({'status': 'healthy', 'checks': checks}, indent=2))
        sys.exit(0)
    else:
        print(json.dumps({'status': 'unhealthy', 'checks': checks}, indent=2))
        sys.exit(1)

if __name__ == "__main__":
    main()
EOF

# Make scripts executable and set ownership
RUN chmod +x /opt/airflow/entrypoint.sh /opt/airflow/healthcheck.py

# Set the entrypoint
ENTRYPOINT ["/opt/airflow/entrypoint.sh"]