# ================================================================================================
# BIGDATA PROCESSING SYSTEM - DLT WORKER DOCKERFILE (PRODUCTION)
# ================================================================================================
# Production-optimized dlt worker with Java support for HDFS/Hadoop connectors
# Multi-stage build with security hardening and comprehensive error handling
# ================================================================================================

FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive

# ================================================================================================
# DEPENDENCIES STAGE
# ================================================================================================
FROM base AS dependencies

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    libpq-dev \
    libxml2-dev \
    libxslt1-dev \
    libffi-dev \
    libjpeg-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy and install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# ================================================================================================
# JAVA STAGE - Optimized Java installation
# ================================================================================================
FROM eclipse-temurin:11-jre-alpine AS java-base

# ================================================================================================
# PRODUCTION STAGE
# ================================================================================================
FROM base AS production

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    unzip \
    postgresql-client \
    redis-tools \
    libpq5 \
    libxml2 \
    libxslt1.1 \
    procps \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy Java from java-base stage
COPY --from=java-base /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=${JAVA_HOME}/bin:${PATH}

# Verify Java installation
RUN java -version

# Create non-root user for security
RUN groupadd --gid 1001 dltuser && \
    useradd --uid 1001 --gid dltuser --shell /bin/bash --create-home dltuser

# Copy Python dependencies from dependencies stage
COPY --from=dependencies /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=dependencies /usr/local/bin /usr/local/bin

# Set working directory
WORKDIR /app

# Create necessary directories with proper permissions
RUN mkdir -p /app/data/{input,temp,processed} \
    /app/logs \
    /app/.dlt \
    /tmp/dlt && \
    chown -R dltuser:dltuser /app /tmp/dlt && \
    chmod -R 755 /app/data && \
    chmod -R 755 /app/logs && \
    chmod -R 755 /app/.dlt

# Copy application code
COPY --chown=dltuser:dltuser . .

# Create dlt configuration template
RUN cat > /app/.dlt/config.toml << 'EOF'
[runtime]
log_level = "INFO"
progress_reporter = "tqdm"

[normalize]
max_nesting_level = 10

[pipeline]
enable_runtime_stats = true

[sources]
csv_delimiter = "auto"
json_variant = "typed"

# Performance settings for large files
[load]
workers = 4
parallelism = "thread_pool"

# Memory optimization
[extract.memory]
limit_mb = 2048
buffer_size_mb = 128
EOF

# Create startup script with comprehensive validation and monitoring
RUN cat > /app/start.sh << 'EOF'
#!/bin/bash
set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting BigData Processing dlt Worker...${NC}"

# Function to log with timestamp and color
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

# Validate required environment variables
log "Validating environment variables..."
required_vars=("DATABASE_URL" "REDIS_URL")
missing_vars=()

for var in "${required_vars[@]}"; do
    if [[ -z "${!var}" ]]; then
        missing_vars+=("$var")
    fi
done

if [[ ${#missing_vars[@]} -gt 0 ]]; then
    error_log "Missing required environment variables: ${missing_vars[*]}"
    exit 1
fi

# Validate Java installation
log "Validating Java installation..."
if ! java -version >/dev/null 2>&1; then
    error_log "Java is not properly installed or configured"
    exit 1
else
    info_log "Java validation successful: $(java -version 2>&1 | head -n 1)"
fi

# Test database connectivity
log "Testing database connectivity..."
timeout 30 bash -c 'until pg_isready -h postgres -p 5432 -U ${POSTGRES_USER:-bigdata_user}; do
    echo "Waiting for PostgreSQL..."
    sleep 2
done' || {
    error_log "Database connectivity check failed"
    exit 1
}

# Test Redis connectivity
log "Testing Redis connectivity..."
timeout 30 bash -c 'until redis-cli -h redis -p 6379 ping | grep -q PONG; do
    echo "Waiting for Redis..."
    sleep 2
done' 2>/dev/null || {
    warn_log "Redis connectivity check failed - continuing anyway"
}

# Test ClickHouse connectivity
if [[ -n "${CLICKHOUSE_URL}" ]]; then
    log "Testing ClickHouse connectivity..."
    timeout 30 bash -c 'until curl -s ${CLICKHOUSE_URL}/ping | grep -q "Ok"; do
        echo "Waiting for ClickHouse..."
        sleep 2
    done' || {
        warn_log "ClickHouse connectivity check failed - continuing anyway"
    }
fi

# Test HDFS connectivity
if [[ -n "${HDFS_NAMENODE_URL}" ]]; then
    log "Testing HDFS connectivity..."
    timeout 30 bash -c 'until curl -s ${HDFS_NAMENODE_URL}/jmx | grep -q "NameNode"; do
        echo "Waiting for HDFS Namenode..."
        sleep 2
    done' || {
        warn_log "HDFS connectivity check failed - continuing anyway"
    }
fi

# Validate dlt installation and dependencies
log "Validating dlt installation..."
python -c "
import dlt
import pandas
import pyarrow
import sqlalchemy
print(f'dlt version: {dlt.__version__}')
print('All required packages are available')
" || {
    error_log "dlt or required dependencies validation failed"
    exit 1
}

# Set up dlt configuration from environment variables
log "Setting up dlt configuration..."
cat > /app/.dlt/secrets.toml << EOFCONFIG
[destination.postgres.credentials]
database = "${POSTGRES_DB:-bigdata_db}"
username = "${POSTGRES_USER:-bigdata_user}"
password = "${POSTGRES_PASSWORD}"
host = "postgres"
port = 5432

[destination.clickhouse.credentials]
host = "clickhouse"
port = 8123
username = "${CLICKHOUSE_USER:-bigdata_user}"
password = "${CLICKHOUSE_PASSWORD}"

[destination.filesystem]
bucket_url = "webhdfs://namenode:50070/user/dlt/landing"

[sources.redis.credentials]
host = "redis"
port = 6379
EOFCONFIG

# Create log directory if it doesn't exist
mkdir -p /app/logs

# Set proper permissions
chmod 755 /app/logs
chmod 755 /app/data
chmod 644 /app/.dlt/secrets.toml

log "Environment validation completed successfully"

# Monitor system resources
info_log "System Resources:"
info_log "Memory: $(free -h | grep '^Mem:' | awk '{print $2 " total, " $7 " available"}')"
info_log "Disk Space: $(df -h /app | tail -1 | awk '{print $4 " available"}')"
info_log "Java Heap: -Xmx$(echo $JAVA_OPTS | grep -o '\-Xmx[^ ]*' | sed 's/-Xmx//' || echo 'default')"

log "Starting dlt worker application..."

# Start the application with monitoring
exec python -m app.main
EOF

# Create health check script
RUN cat > /app/healthcheck.py << 'EOF'
#!/usr/bin/env python3
"""
Health check script for dlt worker
"""
import sys
import os
import subprocess
import json
from datetime import datetime

def check_python_imports():
    """Check if required Python modules can be imported"""
    try:
        import dlt
        import pandas
        import pyarrow
        return True, f"dlt {dlt.__version__}"
    except ImportError as e:
        return False, f"Import error: {e}"

def check_java():
    """Check if Java is available and working"""
    try:
        result = subprocess.run(['java', '-version'],
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            version = result.stderr.split('\n')[0] if result.stderr else "Unknown"
            return True, version
        return False, f"Java exit code: {result.returncode}"
    except Exception as e:
        return False, f"Java check failed: {e}"

def check_data_directories():
    """Check if data directories are accessible"""
    dirs = ['/app/data/input', '/app/data/temp', '/app/data/processed', '/app/logs']
    for dir_path in dirs:
        if not os.path.exists(dir_path):
            return False, f"Directory missing: {dir_path}"
        if not os.access(dir_path, os.W_OK):
            return False, f"Directory not writable: {dir_path}"
    return True, "All directories accessible"

def main():
    checks = {
        'timestamp': datetime.utcnow().isoformat(),
        'python_imports': check_python_imports(),
        'java': check_java(),
        'data_directories': check_data_directories(),
    }

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

# Make scripts executable
RUN chmod +x /app/start.sh /app/healthcheck.py && \
    chown dltuser:dltuser /app/start.sh /app/healthcheck.py

# Switch to non-root user
USER dltuser

# Health check using Python script
HEALTHCHECK --interval=30s --timeout=15s --start-period=60s --retries=3 \
    CMD python /app/healthcheck.py || exit 1

# Use startup script
CMD ["/app/start.sh"]