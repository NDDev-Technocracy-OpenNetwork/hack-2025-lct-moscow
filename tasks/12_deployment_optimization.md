# Задача 12: Deployment & Optimization

## Цель
Оптимизация системы для production deployment, настройка monitoring, logging, security и performance tuning для обработки больших данных.

## Технические требования
- Production-ready Docker setup
- Monitoring и alerting система
- Centralized logging
- Security hardening
- Performance optimization
- Scalability improvements

## Production Docker Setup

### docker-compose.prod.yml
```yaml
version: '3.8'

networks:
  bigdata_network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.0.0/16

volumes:
  postgres_data:
  clickhouse_data:
  namenode_data:
  datanode_data:
  redis_data:
  airflow_logs:
  grafana_data:
  prometheus_data:

services:
  # Traefik Load Balancer & SSL Termination
  traefik:
    image: traefik:v3.0
    container_name: traefik
    restart: unless-stopped
    command:
      - --api.dashboard=true
      - --api.insecure=true
      - --providers.docker=true
      - --providers.docker.exposedbydefault=false
      - --entrypoints.web.address=:80
      - --entrypoints.websecure.address=:443
      - --certificatesresolvers.myresolver.acme.tlschallenge=true
      - --certificatesresolvers.myresolver.acme.email=admin@example.com
      - --certificatesresolvers.myresolver.acme.storage=/letsencrypt/acme.json
    ports:
      - "80:80"
      - "443:443"
      - "8080:8080"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - ./letsencrypt:/letsencrypt
    networks:
      - bigdata_network

  # Frontend (React)
  frontend:
    build:
      context: ./services/frontend
      dockerfile: Dockerfile.prod
    container_name: bigdata_frontend
    restart: unless-stopped
    environment:
      - NODE_ENV=production
      - REACT_APP_API_URL=https://api.bigdata.local
      - REACT_APP_WS_URL=wss://api.bigdata.local
    labels:
      - traefik.enable=true
      - traefik.http.routers.frontend.rule=Host(`bigdata.local`)
      - traefik.http.routers.frontend.entrypoints=websecure
      - traefik.http.routers.frontend.tls.certresolver=myresolver
      - traefik.http.services.frontend.loadbalancer.server.port=80
    depends_on:
      - backend
    networks:
      - bigdata_network

  # Backend API (FastAPI)
  backend:
    build:
      context: ./services/backend
      dockerfile: Dockerfile.prod
    container_name: bigdata_backend
    restart: unless-stopped
    environment:
      - ENVIRONMENT=production
      - DATABASE_URL=postgresql://bigdata_user:${POSTGRES_PASSWORD}@postgres:5432/bigdata_db
      - REDIS_URL=redis://redis:6379
      - CLICKHOUSE_URL=clickhouse://bigdata_user:${CLICKHOUSE_PASSWORD}@clickhouse:8123/analytics
      - SECRET_KEY=${SECRET_KEY}
      - CORS_ORIGINS=https://bigdata.local
      - LOG_LEVEL=info
      - SENTRY_DSN=${SENTRY_DSN}
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    labels:
      - traefik.enable=true
      - traefik.http.routers.backend.rule=Host(`api.bigdata.local`)
      - traefik.http.routers.backend.entrypoints=websecure
      - traefik.http.routers.backend.tls.certresolver=myresolver
      - traefik.http.services.backend.loadbalancer.server.port=8000
    depends_on:
      - postgres
      - redis
      - clickhouse
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    networks:
      - bigdata_network

  # DLT Worker (Scalable)
  dlt-worker:
    build:
      context: ./services/dlt-worker
      dockerfile: Dockerfile.prod
    restart: unless-stopped
    environment:
      - ENVIRONMENT=production
      - DATABASE_URL=postgresql://bigdata_user:${POSTGRES_PASSWORD}@postgres:5432/bigdata_db
      - CLICKHOUSE_URL=clickhouse://bigdata_user:${CLICKHOUSE_PASSWORD}@clickhouse:8123/analytics
      - HDFS_NAMENODE_URL=hdfs://namenode:9000
      - LOG_LEVEL=info
      - WORKER_MEMORY_LIMIT=4g
      - WORKER_CPU_LIMIT=2
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    depends_on:
      - postgres
      - clickhouse
      - namenode
    deploy:
      replicas: 3
      resources:
        limits:
          cpus: '2.0'
          memory: 4G
        reservations:
          cpus: '1.0'
          memory: 2G
    networks:
      - bigdata_network

  # PostgreSQL with replication
  postgres:
    image: postgres:15-alpine
    container_name: bigdata_postgres
    restart: unless-stopped
    environment:
      - POSTGRES_DB=bigdata_db
      - POSTGRES_USER=bigdata_user
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      - POSTGRES_INITDB_ARGS=--auth-host=md5
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./config/postgres/postgresql.conf:/etc/postgresql/postgresql.conf
      - ./config/postgres/init.sql:/docker-entrypoint-initdb.d/init.sql
    command: postgres -c config_file=/etc/postgresql/postgresql.conf
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U bigdata_user -d bigdata_db"]
      interval: 30s
      timeout: 10s
      retries: 5
    networks:
      - bigdata_network

  # ClickHouse for Analytics
  clickhouse:
    image: clickhouse/clickhouse-server:23.8-alpine
    container_name: bigdata_clickhouse
    restart: unless-stopped
    environment:
      - CLICKHOUSE_DB=analytics
      - CLICKHOUSE_USER=bigdata_user
      - CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD}
    volumes:
      - clickhouse_data:/var/lib/clickhouse
      - ./config/clickhouse/config.xml:/etc/clickhouse-server/config.xml
      - ./config/clickhouse/users.xml:/etc/clickhouse-server/users.xml
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
    healthcheck:
      test: ["CMD", "clickhouse-client", "--query", "SELECT 1"]
      interval: 30s
      timeout: 10s
      retries: 5
    networks:
      - bigdata_network

  # HDFS NameNode
  namenode:
    image: apache/hadoop:3.3.4
    container_name: bigdata_namenode
    restart: unless-stopped
    command: ["hdfs", "namenode"]
    environment:
      - CLUSTER_NAME=bigdata_cluster
      - HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
    volumes:
      - namenode_data:/hadoop/dfs/name
      - ./config/hdfs/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml
      - ./config/hdfs/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9870"]
      interval: 30s
      timeout: 10s
      retries: 5
    networks:
      - bigdata_network

  # HDFS DataNode
  datanode:
    image: apache/hadoop:3.3.4
    container_name: bigdata_datanode
    restart: unless-stopped
    command: ["hdfs", "datanode"]
    environment:
      - HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
    volumes:
      - datanode_data:/hadoop/dfs/data
      - ./config/hdfs/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml
      - ./config/hdfs/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml
    depends_on:
      - namenode
    networks:
      - bigdata_network

  # Airflow Webserver
  airflow-webserver:
    build:
      context: ./services/airflow
      dockerfile: Dockerfile.prod
    container_name: bigdata_airflow_webserver
    restart: unless-stopped
    command: webserver
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://airflow_user:${AIRFLOW_PASSWORD}@postgres:5432/airflow_db
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
      - AIRFLOW__WEBSERVER__SECRET_KEY=${AIRFLOW_SECRET_KEY}
    volumes:
      - ./services/airflow/dags:/opt/airflow/dags
      - airflow_logs:/opt/airflow/logs
    depends_on:
      - postgres
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 5
    labels:
      - traefik.enable=true
      - traefik.http.routers.airflow.rule=Host(`airflow.bigdata.local`)
      - traefik.http.routers.airflow.entrypoints=websecure
      - traefik.http.routers.airflow.tls.certresolver=myresolver
      - traefik.http.services.airflow.loadbalancer.server.port=8080
    networks:
      - bigdata_network

  # Airflow Scheduler
  airflow-scheduler:
    build:
      context: ./services/airflow
      dockerfile: Dockerfile.prod
    container_name: bigdata_airflow_scheduler
    restart: unless-stopped
    command: scheduler
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://airflow_user:${AIRFLOW_PASSWORD}@postgres:5432/airflow_db
    volumes:
      - ./services/airflow/dags:/opt/airflow/dags
      - airflow_logs:/opt/airflow/logs
    depends_on:
      - postgres
      - airflow-webserver
    networks:
      - bigdata_network

  # Redis for caching and queues
  redis:
    image: redis:7-alpine
    container_name: bigdata_redis
    restart: unless-stopped
    command: redis-server --requirepass ${REDIS_PASSWORD} --maxmemory 512mb --maxmemory-policy allkeys-lru
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "--raw", "incr", "ping"]
      interval: 30s
      timeout: 10s
      retries: 5
    networks:
      - bigdata_network

  # Prometheus for metrics
  prometheus:
    image: prom/prometheus:latest
    container_name: bigdata_prometheus
    restart: unless-stopped
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.console.libraries=/etc/prometheus/console_libraries'
      - '--web.console.templates=/etc/prometheus/consoles'
      - '--storage.tsdb.retention.time=200h'
      - '--web.enable-lifecycle'
    volumes:
      - prometheus_data:/prometheus
      - ./config/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
    labels:
      - traefik.enable=true
      - traefik.http.routers.prometheus.rule=Host(`prometheus.bigdata.local`)
      - traefik.http.routers.prometheus.entrypoints=websecure
      - traefik.http.routers.prometheus.tls.certresolver=myresolver
      - traefik.http.services.prometheus.loadbalancer.server.port=9090
    networks:
      - bigdata_network

  # Grafana for visualization
  grafana:
    image: grafana/grafana:latest
    container_name: bigdata_grafana
    restart: unless-stopped
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_PASSWORD}
      - GF_USERS_ALLOW_SIGN_UP=false
    volumes:
      - grafana_data:/var/lib/grafana
      - ./config/grafana/provisioning:/etc/grafana/provisioning
      - ./config/grafana/dashboards:/var/lib/grafana/dashboards
    depends_on:
      - prometheus
    labels:
      - traefik.enable=true
      - traefik.http.routers.grafana.rule=Host(`grafana.bigdata.local`)
      - traefik.http.routers.grafana.entrypoints=websecure
      - traefik.http.routers.grafana.tls.certresolver=myresolver
      - traefik.http.services.grafana.loadbalancer.server.port=3000
    networks:
      - bigdata_network

  # Node Exporter for system metrics
  node-exporter:
    image: prom/node-exporter:latest
    container_name: bigdata_node_exporter
    restart: unless-stopped
    volumes:
      - /proc:/host/proc:ro
      - /sys:/host/sys:ro
      - /:/rootfs:ro
    command:
      - '--path.procfs=/host/proc'
      - '--path.rootfs=/rootfs'
      - '--path.sysfs=/host/sys'
      - '--collector.filesystem.mount-points-exclude=^/(sys|proc|dev|host|etc)($$|/)'
    networks:
      - bigdata_network

  # cAdvisor for container metrics
  cadvisor:
    image: gcr.io/cadvisor/cadvisor:latest
    container_name: bigdata_cadvisor
    restart: unless-stopped
    volumes:
      - /:/rootfs:ro
      - /var/run:/var/run:rw
      - /sys:/sys:ro
      - /var/lib/docker/:/var/lib/docker:ro
    networks:
      - bigdata_network

  # Elasticsearch for logging
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.11.0
    container_name: bigdata_elasticsearch
    restart: unless-stopped
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
      - "ES_JAVA_OPTS=-Xms2g -Xmx2g"
    volumes:
      - elasticsearch_data:/usr/share/elasticsearch/data
    ulimits:
      memlock:
        soft: -1
        hard: -1
    networks:
      - bigdata_network

  # Kibana for log visualization
  kibana:
    image: docker.elastic.co/kibana/kibana:8.11.0
    container_name: bigdata_kibana
    restart: unless-stopped
    environment:
      - ELASTICSEARCH_HOSTS=http://elasticsearch:9200
    depends_on:
      - elasticsearch
    labels:
      - traefik.enable=true
      - traefik.http.routers.kibana.rule=Host(`kibana.bigdata.local`)
      - traefik.http.routers.kibana.entrypoints=websecure
      - traefik.http.routers.kibana.tls.certresolver=myresolver
      - traefik.http.services.kibana.loadbalancer.server.port=5601
    networks:
      - bigdata_network

  # Filebeat for log shipping
  filebeat:
    image: docker.elastic.co/beats/filebeat:8.11.0
    container_name: bigdata_filebeat
    restart: unless-stopped
    user: root
    volumes:
      - ./config/filebeat/filebeat.yml:/usr/share/filebeat/filebeat.yml:ro
      - ./logs:/var/log/bigdata:ro
      - /var/lib/docker/containers:/var/lib/docker/containers:ro
      - /var/run/docker.sock:/var/run/docker.sock:ro
    depends_on:
      - elasticsearch
    networks:
      - bigdata_network

volumes:
  elasticsearch_data:
```

## Production Optimized Dockerfiles

### services/backend/Dockerfile.prod
```dockerfile
# Multi-stage build for production
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Production stage
FROM python:3.11-slim

WORKDIR /app

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy Python dependencies from builder
COPY --from=builder /root/.local /home/appuser/.local

# Make sure scripts in .local are usable
ENV PATH=/home/appuser/.local/bin:$PATH

# Copy application code
COPY app/ ./app/
COPY alembic/ ./alembic/
COPY alembic.ini .

# Set ownership
RUN chown -R appuser:appuser /app
RUN mkdir -p /app/logs /app/data && chown -R appuser:appuser /app/logs /app/data

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run application
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
```

### services/frontend/Dockerfile.prod
```dockerfile
# Build stage
FROM node:18-alpine as builder

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy source code
COPY . .

# Build application
RUN npm run build

# Production stage
FROM nginx:alpine

# Copy built app
COPY --from=builder /app/build /usr/share/nginx/html

# Copy custom nginx config
COPY nginx.prod.conf /etc/nginx/nginx.conf

# Create non-root user
RUN addgroup -g 1001 -S nginx && \
    adduser -S nginx -u 1001

# Set permissions
RUN chown -R nginx:nginx /usr/share/nginx/html && \
    chown -R nginx:nginx /var/cache/nginx && \
    chown -R nginx:nginx /var/log/nginx && \
    chown -R nginx:nginx /etc/nginx/conf.d

# Switch to non-root user
USER nginx

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
```

## Monitoring Configuration

### config/prometheus/prometheus.yml
```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

alerting:
  alertmanagers:
    - static_configs:
        - targets:
          - alertmanager:9093

rule_files:
  - "alerts/*.yml"

scrape_configs:
  # Backend API metrics
  - job_name: 'bigdata-backend'
    static_configs:
      - targets: ['backend:8000']
    metrics_path: '/metrics'
    scrape_interval: 30s

  # System metrics
  - job_name: 'node-exporter'
    static_configs:
      - targets: ['node-exporter:9100']

  # Container metrics
  - job_name: 'cadvisor'
    static_configs:
      - targets: ['cadvisor:8080']

  # PostgreSQL metrics
  - job_name: 'postgres-exporter'
    static_configs:
      - targets: ['postgres-exporter:9187']

  # ClickHouse metrics
  - job_name: 'clickhouse'
    static_configs:
      - targets: ['clickhouse:8123']
    metrics_path: '/metrics'

  # Redis metrics
  - job_name: 'redis-exporter'
    static_configs:
      - targets: ['redis-exporter:9121']

  # Airflow metrics
  - job_name: 'airflow'
    static_configs:
      - targets: ['airflow-webserver:8080']
    metrics_path: '/admin/metrics'
```

### config/grafana/dashboards/bigdata-overview.json
```json
{
  "dashboard": {
    "id": null,
    "title": "BigData Processing Overview",
    "tags": ["bigdata", "etl"],
    "style": "dark",
    "timezone": "browser",
    "panels": [
      {
        "id": 1,
        "title": "Processing Throughput",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(bigdata_records_processed_total[5m])",
            "legendFormat": "Records/sec"
          }
        ],
        "yAxes": [
          {
            "label": "Records per second",
            "min": 0
          }
        ],
        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0}
      },
      {
        "id": 2,
        "title": "Active Jobs",
        "type": "singlestat",
        "targets": [
          {
            "expr": "bigdata_active_jobs",
            "legendFormat": "Active Jobs"
          }
        ],
        "gridPos": {"h": 4, "w": 6, "x": 12, "y": 0}
      },
      {
        "id": 3,
        "title": "System Memory Usage",
        "type": "graph",
        "targets": [
          {
            "expr": "(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100",
            "legendFormat": "Memory Usage %"
          }
        ],
        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8}
      },
      {
        "id": 4,
        "title": "CPU Usage",
        "type": "graph",
        "targets": [
          {
            "expr": "100 - (avg(irate(node_cpu_seconds_total{mode=\"idle\"}[5m])) * 100)",
            "legendFormat": "CPU Usage %"
          }
        ],
        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8}
      },
      {
        "id": 5,
        "title": "Error Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(bigdata_errors_total[5m])",
            "legendFormat": "Errors/sec"
          }
        ],
        "gridPos": {"h": 6, "w": 24, "x": 0, "y": 16}
      }
    ],
    "time": {
      "from": "now-1h",
      "to": "now"
    },
    "refresh": "30s"
  }
}
```

## Logging Configuration

### config/filebeat/filebeat.yml
```yaml
filebeat.inputs:
- type: log
  enabled: true
  paths:
    - /var/log/bigdata/backend/*.log
  fields:
    service: backend
  fields_under_root: true

- type: log
  enabled: true
  paths:
    - /var/log/bigdata/dlt-worker/*.log
  fields:
    service: dlt-worker
  fields_under_root: true

- type: docker
  enabled: true
  containers.ids:
    - "*"
  processors:
    - add_docker_metadata:
        host: "unix:///var/run/docker.sock"

output.elasticsearch:
  hosts: ["elasticsearch:9200"]
  index: "bigdata-logs-%{+yyyy.MM.dd}"

setup.template.name: "bigdata-logs"
setup.template.pattern: "bigdata-logs-*"

processors:
  - add_host_metadata:
      when.not.contains.tags: forwarded
  - add_cloud_metadata: ~
  - add_docker_metadata: ~

logging.level: info
logging.to_files: true
logging.files:
  path: /var/log/filebeat
  name: filebeat
  keepfiles: 7
  permissions: 0644
```

## Security Hardening

### config/postgres/postgresql.conf
```ini
# Connection settings
listen_addresses = '*'
port = 5432
max_connections = 200
superuser_reserved_connections = 3

# Authentication
ssl = on
ssl_cert_file = '/etc/ssl/certs/server.crt'
ssl_key_file = '/etc/ssl/private/server.key'
ssl_ciphers = 'HIGH:MEDIUM:+3DES:!aNULL'
ssl_prefer_server_ciphers = on

# Memory settings
shared_buffers = 512MB
effective_cache_size = 2GB
maintenance_work_mem = 128MB
checkpoint_completion_target = 0.9
wal_buffers = 16MB
default_statistics_target = 100

# Performance tuning
random_page_cost = 1.1
effective_io_concurrency = 200
work_mem = 8MB
min_wal_size = 1GB
max_wal_size = 4GB
max_worker_processes = 8
max_parallel_workers_per_gather = 4
max_parallel_workers = 8

# Logging
log_destination = 'stderr'
logging_collector = on
log_directory = 'pg_log'
log_filename = 'postgresql-%Y-%m-%d_%H%M%S.log'
log_statement = 'mod'
log_min_duration_statement = 1000
log_checkpoints = on
log_connections = on
log_disconnections = on
log_lock_waits = on

# Security
password_encryption = md5
row_security = on
```

## Performance Optimization Scripts

### scripts/optimize_system.sh
```bash
#!/bin/bash

# System optimization script for BigData processing

set -e

echo "🚀 Optimizing system for BigData processing..."

# Increase file descriptor limits
echo "Increasing file descriptor limits..."
echo "* soft nofile 65536" >> /etc/security/limits.conf
echo "* hard nofile 65536" >> /etc/security/limits.conf
echo "session required pam_limits.so" >> /etc/pam.d/common-session

# Optimize kernel parameters for networking
echo "Optimizing network parameters..."
cat << EOF >> /etc/sysctl.conf
# Network optimizations
net.core.rmem_default = 262144
net.core.rmem_max = 16777216
net.core.wmem_default = 262144
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
net.core.netdev_max_backlog = 5000
net.ipv4.tcp_congestion_control = bbr

# Memory optimizations
vm.swappiness = 10
vm.dirty_ratio = 15
vm.dirty_background_ratio = 5
vm.overcommit_memory = 1

# File system optimizations
fs.file-max = 2097152
EOF

# Apply sysctl changes
sysctl -p

# Optimize Docker daemon
echo "Optimizing Docker daemon..."
mkdir -p /etc/docker
cat << EOF > /etc/docker/daemon.json
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  },
  "storage-driver": "overlay2",
  "storage-opts": [
    "overlay2.override_kernel_check=true"
  ],
  "default-runtime": "runc",
  "runtimes": {
    "runc": {
      "path": "runc"
    }
  },
  "default-shm-size": "128M",
  "userland-proxy": false,
  "experimental": false,
  "metrics-addr": "0.0.0.0:9323",
  "live-restore": true
}
EOF

# Restart Docker
systemctl restart docker

# Setup log rotation
echo "Setting up log rotation..."
cat << EOF > /etc/logrotate.d/bigdata
/var/log/bigdata/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    create 644 root root
    postrotate
        docker kill -s USR1 \$(docker ps -q --filter label=com.docker.compose.service=backend) 2>/dev/null || true
    endscript
}
EOF

# Create monitoring directories
echo "Creating monitoring directories..."
mkdir -p /var/log/bigdata/{backend,dlt-worker,airflow}
chmod 755 /var/log/bigdata
chmod 755 /var/log/bigdata/*

# Setup backup scripts
echo "Setting up backup scripts..."
mkdir -p /opt/bigdata/backups
cat << 'EOF' > /opt/bigdata/backups/backup_databases.sh
#!/bin/bash

BACKUP_DIR="/opt/bigdata/backups"
DATE=$(date +%Y%m%d_%H%M%S)

# Backup PostgreSQL
docker exec bigdata_postgres pg_dump -U bigdata_user bigdata_db > "${BACKUP_DIR}/postgres_backup_${DATE}.sql"

# Backup ClickHouse
docker exec bigdata_clickhouse clickhouse-client --query "BACKUP DATABASE analytics TO Disk('backups', 'clickhouse_backup_${DATE}')"

# Cleanup old backups (keep 7 days)
find "${BACKUP_DIR}" -name "*.sql" -mtime +7 -delete
find "${BACKUP_DIR}" -name "clickhouse_backup_*" -mtime +7 -exec rm -rf {} \;

echo "Backup completed: ${DATE}"
EOF

chmod +x /opt/bigdata/backups/backup_databases.sh

# Setup cron for backups
echo "0 2 * * * /opt/bigdata/backups/backup_databases.sh >> /var/log/backup.log 2>&1" | crontab -

echo "✅ System optimization completed!"
echo "📋 Next steps:"
echo "   1. Reboot the system to apply all changes"
echo "   2. Run: docker-compose -f docker-compose.prod.yml up -d"
echo "   3. Configure monitoring alerts"
echo "   4. Test the system with load testing"
```

## Deployment Scripts

### scripts/deploy.sh
```bash
#!/bin/bash

# Production deployment script

set -e

ENVIRONMENT=${1:-production}
VERSION=${2:-latest}

echo "🚀 Deploying BigData Processing System"
echo "Environment: $ENVIRONMENT"
echo "Version: $VERSION"

# Check if .env file exists
if [ ! -f ".env.${ENVIRONMENT}" ]; then
    echo "❌ Environment file .env.${ENVIRONMENT} not found!"
    exit 1
fi

# Load environment variables
export $(cat .env.${ENVIRONMENT} | grep -v '^#' | xargs)

# Verify required environment variables
required_vars=(
    "POSTGRES_PASSWORD"
    "CLICKHOUSE_PASSWORD"
    "SECRET_KEY"
    "AIRFLOW_PASSWORD"
    "REDIS_PASSWORD"
    "GRAFANA_PASSWORD"
)

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo "❌ Required environment variable $var is not set!"
        exit 1
    fi
done

echo "✅ Environment variables validated"

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p data/{input,temp,processed}
mkdir -p logs/{backend,dlt-worker,airflow}
mkdir -p letsencrypt

# Set permissions
chmod 755 data logs
chmod 755 data/* logs/*

# Build and deploy services
echo "🔨 Building and deploying services..."

# Stop existing services
docker-compose -f docker-compose.prod.yml down --remove-orphans

# Pull latest images
docker-compose -f docker-compose.prod.yml pull

# Build custom services
docker-compose -f docker-compose.prod.yml build --no-cache

# Deploy services
docker-compose -f docker-compose.prod.yml up -d

# Wait for services to be healthy
echo "⏳ Waiting for services to be ready..."
sleep 30

# Check service health
services=(
    "bigdata_postgres"
    "bigdata_clickhouse"
    "bigdata_redis"
    "bigdata_backend"
    "bigdata_frontend"
    "bigdata_airflow_webserver"
)

for service in "${services[@]}"; do
    echo "Checking $service..."
    docker exec $service sh -c 'exit 0' 2>/dev/null || {
        echo "❌ Service $service is not running!"
        exit 1
    }
done

# Run database migrations
echo "🗃️ Running database migrations..."
docker exec bigdata_backend alembic upgrade head

# Initialize Airflow database
echo "✈️ Initializing Airflow..."
docker exec bigdata_airflow_webserver airflow db init
docker exec bigdata_airflow_webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@bigdata.local \
    --password $AIRFLOW_PASSWORD

# Setup Grafana dashboards
echo "📊 Setting up Grafana dashboards..."
sleep 10  # Wait for Grafana to start

# Import dashboards via API
GRAFANA_URL="http://localhost:3000"
GRAFANA_API_KEY=$(docker exec bigdata_grafana grafana-cli admin reset-admin-password $GRAFANA_PASSWORD)

# Health check
echo "🏥 Running health checks..."
services_health=(
    "http://localhost/health:Frontend"
    "http://localhost:8000/health:Backend"
    "http://localhost:8080/health:Airflow"
    "http://localhost:3000/api/health:Grafana"
    "http://localhost:9090/-/healthy:Prometheus"
)

for check in "${services_health[@]}"; do
    url=$(echo $check | cut -d: -f1)
    name=$(echo $check | cut -d: -f2)

    if curl -f -s $url > /dev/null; then
        echo "✅ $name is healthy"
    else
        echo "❌ $name health check failed"
        exit 1
    fi
done

echo "🎉 Deployment completed successfully!"
echo ""
echo "🌐 Service URLs:"
echo "   Frontend: https://bigdata.local"
echo "   API: https://api.bigdata.local"
echo "   Airflow: https://airflow.bigdata.local"
echo "   Grafana: https://grafana.bigdata.local (admin/$GRAFANA_PASSWORD)"
echo "   Prometheus: https://prometheus.bigdata.local"
echo "   Kibana: https://kibana.bigdata.local"
echo ""
echo "📝 Next steps:"
echo "   1. Configure DNS or add entries to /etc/hosts"
echo "   2. Setup SSL certificates"
echo "   3. Configure monitoring alerts"
echo "   4. Run load tests"
echo "   5. Setup backup schedules"
```

## Environment Configuration

### .env.production
```bash
# Database passwords
POSTGRES_PASSWORD=your_super_secure_postgres_password
CLICKHOUSE_PASSWORD=your_secure_clickhouse_password
AIRFLOW_PASSWORD=your_airflow_password
REDIS_PASSWORD=your_redis_password

# Application secrets
SECRET_KEY=your_super_secret_key_for_jwt_and_sessions
AIRFLOW_SECRET_KEY=your_airflow_secret_key

# Monitoring
GRAFANA_PASSWORD=your_grafana_admin_password

# External services
SENTRY_DSN=https://your_sentry_dsn_here

# Scaling settings
BACKEND_WORKERS=4
DLT_WORKER_REPLICAS=3
WORKER_MEMORY_LIMIT=4g
WORKER_CPU_LIMIT=2

# SSL/TLS
ACME_EMAIL=admin@yourdomain.com
DOMAIN=bigdata.yourdomain.com
```

## Задачи для выполнения

### 1. Production Docker Configuration
- [ ] Создать production docker-compose.yml с всеми сервисами
- [ ] Настроить multi-stage Dockerfiles для оптимизации
- [ ] Добавить health checks для всех сервисов
- [ ] Настроить resource limits и reservations

### 2. Load Balancing & SSL
- [ ] Настроить Traefik для load balancing
- [ ] Конфигурация SSL certificates с Let's Encrypt
- [ ] Настройка reverse proxy для всех сервисов
- [ ] Rate limiting и security headers

### 3. Monitoring & Alerting
- [ ] Настроить Prometheus для сбора метрик
- [ ] Создать Grafana dashboards
- [ ] Добавить alerting rules
- [ ] Настроить notification channels

### 4. Centralized Logging
- [ ] ELK stack для централизованного логирования
- [ ] Filebeat для сбора логов
- [ ] Kibana dashboards для анализа логов
- [ ] Log retention policies

### 5. Security Hardening
- [ ] Non-root users в контейнерах
- [ ] Network segmentation
- [ ] Secrets management
- [ ] Database security configuration

### 6. Performance Optimization
- [ ] System-level optimizations
- [ ] Database tuning (PostgreSQL, ClickHouse)
- [ ] Memory и CPU limits
- [ ] Caching strategies

### 7. Backup & Recovery
- [ ] Automated backup scripts
- [ ] Database backup strategies
- [ ] Disaster recovery procedures
- [ ] Backup retention policies

### 8. Deployment Automation
- [ ] Production deployment scripts
- [ ] Environment validation
- [ ] Health check automation
- [ ] Rollback procedures

### 9. Scalability
- [ ] Horizontal scaling для dlt-workers
- [ ] Database replication setup
- [ ] Load testing и capacity planning
- [ ] Auto-scaling policies

### 10. Maintenance & Operations
- [ ] Log rotation configuration
- [ ] System maintenance scripts
- [ ] Performance monitoring
- [ ] Capacity planning tools

## Ожидаемый результат
После выполнения этой задачи должна быть готова production-ready система с полным monitoring, logging, security и scalability для обработки больших данных в enterprise окружении.

## Финальная проверка MVP
- [ ] Система успешно обрабатывает файлы до 10GB
- [ ] Поддерживаются все форматы: CSV, JSON, XML
- [ ] Работают все destinations: PostgreSQL, ClickHouse, HDFS
- [ ] Real-time визуализация процесса через React Flow
- [ ] WebSocket обновления работают корректно
- [ ] Monitoring и alerting настроены
- [ ] Security best practices применены
- [ ] Performance benchmarks пройдены успешно