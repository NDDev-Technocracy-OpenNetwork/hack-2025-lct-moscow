# Backend Dockerfile
# Multi-stage build for FastAPI application

FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive

# Dependencies stage
FROM base AS dependencies

# Install system dependencies required for Python packages
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

# Production stage
FROM base AS production

# Install only runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    postgresql-client \
    redis-tools \
    libpq5 \
    libxml2 \
    libxslt1.1 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user for security
RUN groupadd --gid 1001 appuser && \
    useradd --uid 1001 --gid appuser --shell /bin/bash --create-home appuser

# Copy Python dependencies from dependencies stage
COPY --from=dependencies /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=dependencies /usr/local/bin /usr/local/bin

# Set working directory
WORKDIR /app

# Copy application code first
COPY --chown=appuser:appuser . .

# Create necessary directories with proper permissions
RUN mkdir -p /app/data/{input,temp,processed} \
    /app/logs \
    /tmp/app && \
    chown -R appuser:appuser /app /tmp/app && \
    chmod -R 755 /app/data && \
    chmod -R 755 /app/logs

# Create production configuration file
RUN cat > /app/gunicorn.conf.py << 'EOF'

# GUNICORN PRODUCTION CONFIGURATION

import multiprocessing
import os

# Server socket
bind = "0.0.0.0:8000"
backlog = 2048

# Worker processes
# Use single worker for Socket.IO to avoid cross-process subscription issues
# AsyncIO/Uvicorn can handle many concurrent connections in single process
workers = 1
worker_class = "uvicorn.workers.UvicornWorker"
worker_connections = 1000
max_requests = 10000
max_requests_jitter = 1000
timeout = 600
keepalive = 2

# Security
limit_request_line = 0
limit_request_fields = 100
limit_request_field_size = 0

# Application
wsgi_file = "app.main:socket_app"

# Logging
accesslog = "-"
errorlog = "-"
loglevel = os.getenv("LOG_LEVEL", "info").lower()
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# Process naming
proc_name = "bigdata-backend"

# Server mechanics
daemon = False
pidfile = "/tmp/app/gunicorn.pid"
user = 1001
group = 1001
tmp_upload_dir = "/tmp/app"

# SSL (if needed in production)
# keyfile = "/path/to/keyfile"
# certfile = "/path/to/certfile"

# Worker timeout and graceful timeout
graceful_timeout = 30
worker_tmp_dir = "/dev/shm"
EOF

# Create startup script with proper error handling and environment validation
# Use printf to avoid line ending issues
RUN printf '#!/bin/bash\nset -e\n\n# Color codes for output\nRED='"'"'\\033[0;31m'"'"'\nGREEN='"'"'\\033[0;32m'"'"'\nYELLOW='"'"'\\033[1;33m'"'"'\nNC='"'"'\\033[0m'"'"' # No Color\n\necho -e "${GREEN}Starting BigData Processing Backend...${NC}"\n\n# Function to log with timestamp\nlog() {\n    echo -e "${GREEN}[$(date +'"'"'%%Y-%%m-%%d %%H:%%M:%%S'"'"')] $1${NC}"\n}\n\nerror_log() {\n    echo -e "${RED}[$(date +'"'"'%%Y-%%m-%%d %%H:%%M:%%S'"'"')] ERROR: $1${NC}" >&2\n}\n\nwarn_log() {\n    echo -e "${YELLOW}[$(date +'"'"'%%Y-%%m-%%d %%H:%%M:%%S'"'"')] WARNING: $1${NC}"\n}\n\n# Validate required environment variables\nlog "Validating environment variables..."\nrequired_vars=("DATABASE_URL" "REDIS_URL")\nmissing_vars=()\n\nfor var in "${required_vars[@]}"; do\n    if [[ -z "${!var}" ]]; then\n        missing_vars+=("$var")\n    fi\ndone\n\nif [[ ${#missing_vars[@]} -gt 0 ]]; then\n    error_log "Missing required environment variables: ${missing_vars[*]}"\n    exit 1\nfi\n\n# Test database connectivity\nlog "Testing database connectivity..."\ntimeout 30 bash -c '"'"'until pg_isready -h postgres -p 5432 -U ${POSTGRES_USER:-bigdata_user}; do\n    echo "Waiting for PostgreSQL..."\n    sleep 2\ndone'"'"' || {\n    error_log "Database connectivity check failed"\n    exit 1\n}\n\n# Test Redis connectivity\nlog "Testing Redis connectivity..."\ntimeout 30 bash -c '"'"'until redis-cli -h redis -p 6379 ping | grep -q PONG; do\n    echo "Waiting for Redis..."\n    sleep 2\ndone'"'"' 2>/dev/null || {\n    warn_log "Redis connectivity check failed - continuing anyway"\n}\n\n# Run database migrations if needed\nif [[ "${RUN_MIGRATIONS:-true}" == "true" ]]; then\n    log "Running database migrations..."\n    if [[ -f "alembic.ini" ]]; then\n        python -m alembic upgrade head || {\n            error_log "Database migrations failed"\n            exit 1\n        }\n    else\n        warn_log "No alembic.ini found - skipping database migrations"\n    fi\nfi\n\n# Create log directory if it doesn'"'"'t exist\nmkdir -p /app/logs\n\n# Set proper permissions (non-fatal)\nchmod 755 /app/logs || warn_log "Could not set permissions on /app/logs"\nchmod 755 /app/data || warn_log "Could not set permissions on /app/data"\n\nlog "Environment validation completed successfully"\nlog "Starting application server..."\n\n# Start the application based on environment\nif [[ "${ENVIRONMENT:-production}" == "development" ]]; then\n    warn_log "Running in development mode"\n    exec uvicorn app.main:app \\\n        --host 0.0.0.0 \\\n        --port 8000 \\\n        --reload \\\n        --log-level "${LOG_LEVEL:-info}"\nelse\n    log "Running in production mode with Gunicorn"\n    exec gunicorn -c /app/gunicorn.conf.py app.main:socket_app\nfi\n' > /app/start.sh

# Make startup script executable and set proper ownership
RUN chmod +x /app/start.sh && \
    chown appuser:appuser /app/start.sh && \
    chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 8000

# Health check with improved endpoint
HEALTHCHECK --interval=30s --timeout=15s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Use startup script
CMD ["bash", "/app/start.sh"]