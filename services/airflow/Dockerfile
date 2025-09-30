
# BIGDATA PROCESSING SYSTEM - AIRFLOW ALL-IN-ONE DOCKERFILE

# Production-optimized Apache Airflow with webserver + scheduler in single container


FROM apache/airflow:2.7.3-python3.11

# Switch to root for system installations
USER root

# Install system dependencies including dos2unix for line ending conversion
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    postgresql-client \
    redis-tools \
    procps \
    dos2unix \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create additional directories
RUN mkdir -p \
    /opt/airflow/dags \
    /opt/airflow/logs \
    /opt/airflow/plugins \
    && chown -R airflow:root /opt/airflow

# Switch back to airflow user for remaining operations
USER airflow

# Copy requirements and install dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Copy application files
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root plugins/ /opt/airflow/plugins/

# Switch back to root to configure entrypoint
USER root

# Copy and configure entrypoint script
COPY entrypoint.sh /entrypoint.sh

# Convert line endings from Windows (CRLF) to Unix (LF) and make executable
RUN dos2unix /entrypoint.sh && chmod +x /entrypoint.sh

# Switch back to airflow user
USER airflow

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Expose port
EXPOSE 8080

# Set entrypoint and default command
ENTRYPOINT ["/entrypoint.sh"]
CMD ["standalone"]