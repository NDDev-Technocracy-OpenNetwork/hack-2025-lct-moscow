# CLAUDE.md

Technical reference for Claude Code when working with this repository.

## Project Overview

Distributed data processing system using Apache Airflow orchestration, dlt (Data Load Tool) for ETL pipelines, and multiple destination support (PostgreSQL, ClickHouse, HDFS). Microservices architecture with FastAPI backend, React frontend, and WebSocket real-time updates.

## Architecture

### Services
- **frontend**: React.js application with React Flow visualization (port 3000)
- **backend**: FastAPI async application with WebSocket support (port 8000)
- **dlt-worker**: dlt ETL processing worker with Java/HDFS support
- **postgres**: PostgreSQL 15 database for metadata and Airflow (port 5432)
- **clickhouse**: ClickHouse analytics database (ports 8123, 9001)
- **namenode**: HDFS namenode (ports 9000, 9870)
- **datanode**: HDFS datanode
- **airflow**: Apache Airflow standalone mode (port 8080, optional profile)
- **redis**: Redis cache and message broker (port 6379)
- **pgadmin**: PostgreSQL admin interface (port 5050, optional profile)

### Technology Stack
- **Backend**: Python 3.11, FastAPI, SQLAlchemy, asyncpg, Pydantic, WebSockets
- **Frontend**: React.js, React Flow, Axios, Socket.IO client
- **ETL**: dlt (Data Load Tool), Apache Airflow
- **Databases**: PostgreSQL, ClickHouse, HDFS
- **Infrastructure**: Docker, Docker Compose, Nginx, Gunicorn, Uvicorn

### Directory Structure
```
.
├── services/
│   ├── backend/          # FastAPI application
│   ├── frontend/         # React application
│   ├── dlt-worker/       # dlt ETL worker
│   └── airflow/          # Airflow DAGs and config
├── config/               # Service configurations
│   ├── postgres/
│   ├── clickhouse/
│   └── hdfs/
├── data/                 # Persistent data storage
│   ├── input/
│   ├── temp/
│   └── processed/
├── logs/                 # Service logs
├── tasks/                # Project task definitions
├── docker-compose.yml    # Service orchestration
├── .env                  # Environment configuration
└── Makefile              # Common operations
```

## Development Commands

### Docker Operations
```bash
# Start all core services
docker-compose up -d

# Start with Airflow (requires profile)
docker-compose --profile airflow up -d

# Start with optional tools (pgadmin)
docker-compose --profile tools up -d

# Rebuild specific service
docker-compose build <service-name>

# View logs
docker-compose logs -f <service-name>
docker-compose logs -f --tail 50 backend

# Restart service
docker-compose restart <service-name>

# Stop all services
docker-compose down

# Stop and remove volumes (data loss)
docker-compose down -v

# Check service health
docker-compose ps
```

### Backend Development
```bash
# Enter backend container
docker-compose exec backend bash

# Run migrations (if alembic configured)
docker-compose exec backend alembic upgrade head

# Check database connectivity
docker-compose exec backend python -c "from app.database import engine; print('OK')"

# View backend logs
docker-compose logs -f backend

# Restart backend after code changes
docker-compose restart backend
```

### Database Operations
```bash
# Access PostgreSQL CLI
docker-compose exec postgres psql -U bigdata_user -d bigdata_db

# Access ClickHouse CLI
docker-compose exec clickhouse clickhouse-client

# Access Redis CLI
docker-compose exec redis redis-cli

# Backup PostgreSQL database
docker-compose exec postgres pg_dump -U bigdata_user bigdata_db > backup.sql

# Restore PostgreSQL database
docker-compose exec -T postgres psql -U bigdata_user bigdata_db < backup.sql
```

### Airflow Operations
```bash
# Start Airflow (standalone mode)
docker-compose --profile airflow up -d airflow

# Access Airflow CLI
docker-compose exec airflow airflow --help

# List DAGs
docker-compose exec airflow airflow dags list

# Trigger DAG manually
docker-compose exec airflow airflow dags trigger <dag-id>

# View Airflow logs
docker-compose logs -f airflow
```

### HDFS Operations
```bash
# Access HDFS CLI
docker-compose exec namenode bash

# List HDFS files
docker-compose exec namenode hdfs dfs -ls /

# Create directory
docker-compose exec namenode hdfs dfs -mkdir -p /data/input

# Upload file to HDFS
docker-compose exec namenode hdfs dfs -put /local/path /hdfs/path

# Check HDFS health
curl http://localhost:9870/dfshealth.html
```

### Testing and Debugging
```bash
# Check all service health
docker-compose ps

# Test backend API
curl http://localhost:8000/health
curl http://localhost:8000/docs

# Test WebSocket connection
wscat -c ws://localhost:8000/ws/test-client-id

# View all logs
docker-compose logs -f

# Check resource usage
docker stats

# Inspect container
docker-compose exec <service> bash
```

### Cleanup Operations
```bash
# Remove stopped containers
docker-compose rm -f

# Prune unused volumes (careful - data loss)
docker volume prune

# Prune unused networks
docker network prune

# Full cleanup (removes all data)
docker-compose down -v
docker system prune -a --volumes
```

## Configuration

### Environment Variables
Configuration managed via `.env` file (see `.env.example` for template):

**Database Configuration**:
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
- `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DB`
- `DATABASE_URL` - PostgreSQL connection string
- `REDIS_URL` - Redis connection string

**Service URLs**:
- `BACKEND_EXTERNAL_URL` - Backend URL for frontend
- `WEBSOCKET_EXTERNAL_URL` - WebSocket URL for frontend
- `CLICKHOUSE_URL` - ClickHouse HTTP endpoint
- `HDFS_NAMENODE_URL` - HDFS namenode URL

**Airflow Configuration**:
- `AIRFLOW_EXECUTOR` - Executor type (LocalExecutor, CeleryExecutor)
- `AIRFLOW_FERNET_KEY` - Encryption key for sensitive data
- `AIRFLOW_WEBSERVER_SECRET_KEY` - Webserver session key
- `AIRFLOW_ADMIN_USERNAME`, `AIRFLOW_ADMIN_PASSWORD`

**Application Settings**:
- `LOG_LEVEL` - Logging level (DEBUG, INFO, WARNING, ERROR)
- `ENVIRONMENT` - Environment name (development, production)
- `PYTHONPATH` - Python module search path

### Service Access
- **Frontend**: http://localhost:3000
- **Backend API**: http://localhost:8000
- **Backend Docs**: http://localhost:8000/docs
- **Airflow UI**: http://localhost:8080 (requires `--profile airflow`)
- **HDFS UI**: http://localhost:9870
- **ClickHouse HTTP**: http://localhost:8123
- **PgAdmin**: http://localhost:5050 (requires `--profile tools`)

### Database Connections
**PostgreSQL**:
```
Host: localhost
Port: 5432
Database: bigdata_db
User: bigdata_user
Password: bigdata_pass
```

**ClickHouse**:
```
HTTP: http://localhost:8123
Native: localhost:9001
Database: default
```

**Redis**:
```
URL: redis://localhost:6379/0
```

## Code Standards

### General Rules
- NO emojis in source code or logs
- NO marketing language (e.g., "amazing", "powerful", "revolutionary")
- NO excessive decorative comments (e.g., rows of equals signs)
- Use technical, descriptive language in English
- Comments should explain "why", not "what"
- Configuration via environment variables (12-factor app)
- Type hints required for all Python functions
- Async/await patterns for I/O operations

### Python Standards
- Python 3.11+ compatibility required
- Type hints mandatory (mypy enforced)
- Pydantic for data validation
- FastAPI dependency injection patterns
- SQLAlchemy async patterns with asyncpg
- Proper exception handling with logging
- Line length: 100 characters max
- Import order: standard library, third-party, local

### Docker Standards
- Multi-stage builds for production images
- Non-root users in containers (security)
- Health checks for all services
- Volume mounts for persistent data
- Network isolation via Docker networks
- Environment-based configuration
- Proper signal handling (SIGTERM)

### API Standards
- REST endpoints follow `/api/v1/` prefix
- WebSocket endpoints use `/ws/` prefix
- Health check endpoint at `/health`
- OpenAPI documentation at `/docs`
- Pydantic schemas for request/response validation
- Proper HTTP status codes
- CORS configuration via environment

### Database Standards
- Async database operations (asyncpg)
- Connection pooling enabled
- Migrations managed via Alembic
- Foreign key constraints enforced
- Proper indexing for queries
- Transaction management for consistency
- Prepared statements (SQL injection prevention)

### Testing Standards
- Unit tests for business logic
- Integration tests for API endpoints
- Health check tests for services
- Mock external dependencies
- Test coverage minimum 80%
- Use pytest framework
- Test data fixtures via pytest

## Common Issues and Solutions

### Backend Database Connection Error
**Problem**: `password authentication failed for user bigdata_user`

**Solution**:
```bash
# Reset PostgreSQL password
docker-compose exec postgres psql -U bigdata_user -d bigdata_db -c "ALTER USER bigdata_user WITH PASSWORD 'bigdata_pass';"

# Restart backend
docker-compose restart backend
```

### Airflow Not Starting
**Problem**: Airflow container not visible in `docker-compose ps`

**Solution**: Airflow uses optional profile
```bash
docker-compose --profile airflow up -d airflow
```

### ClickHouse Unhealthy
**Problem**: Health check failing due to IPv6 resolution

**Solution**: Health checks use `127.0.0.1` (IPv4) instead of `localhost`
```yaml
healthcheck:
  test: ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://127.0.0.1:8123/ping"]
```

### HDFS Datanode Not Registering
**Problem**: Datanode cannot connect to namenode

**Solution**:
```bash
# Check namenode is healthy first
docker-compose ps namenode

# Restart datanode
docker-compose restart datanode

# Check namenode logs for registration
docker-compose logs namenode | grep datanode
```

### Frontend Cannot Connect to Backend
**Problem**: CORS errors in browser console

**Solution**: Verify CORS origins configured in `.env`
```bash
BACKEND_CORS_ORIGINS=http://localhost:3000,http://frontend:3000
```

### Port Already in Use
**Problem**: `bind: address already in use`

**Solution**:
```bash
# Find process using port
lsof -i :8000  # macOS/Linux
netstat -ano | findstr :8000  # Windows

# Stop conflicting process or change port in .env
```

## Development Workflow

### Adding New Service
1. Create Dockerfile in `services/<service-name>/`
2. Add service definition to `docker-compose.yml`
3. Add environment variables to `.env`
4. Configure health check for service
5. Add service to network: `bigdata-network`
6. Update this documentation

### Database Schema Changes
1. Modify SQLAlchemy models in `services/backend/app/models/`
2. Generate Alembic migration: `alembic revision --autogenerate -m "description"`
3. Review generated migration in `alembic/versions/`
4. Apply migration: `alembic upgrade head`
5. Test rollback: `alembic downgrade -1`

### Adding New API Endpoint
1. Create route in `services/backend/app/api/v1/`
2. Define Pydantic schemas for request/response
3. Implement business logic with proper error handling
4. Add route to API router in `__init__.py`
5. Test endpoint via `/docs` or curl
6. Update frontend client if needed

### Creating Airflow DAG
1. Create DAG file in `services/airflow/dags/`
2. Define tasks with proper dependencies
3. Configure schedule interval and catchup
4. Test DAG: `docker-compose exec airflow airflow dags test <dag-id> <execution-date>`
5. Enable DAG in Airflow UI
6. Monitor execution logs

### Adding dlt Pipeline
1. Create pipeline script in `services/dlt-worker/pipelines/`
2. Define dlt source with `@dlt.source` decorator
3. Define resources with `@dlt.resource` decorator
4. Configure destination in pipeline
5. Test pipeline execution locally
6. Integrate with Airflow DAG if needed

## Security Considerations

- Change default passwords in `.env` for production
- Rotate `AIRFLOW_FERNET_KEY` and `AIRFLOW_WEBSERVER_SECRET_KEY`
- Use secrets management system (Vault, AWS Secrets Manager)
- Never commit `.env` file to version control
- Enable PostgreSQL SSL in production
- Implement API authentication (OAuth2, JWT)
- Configure firewall rules for exposed ports
- Regular security updates for base images
- Monitor logs for suspicious activity
- Implement rate limiting for API endpoints

## Performance Optimization

### Backend
- Use connection pooling (SQLAlchemy default)
- Implement Redis caching for frequent queries
- Use async operations for I/O bound tasks
- Configure Gunicorn workers based on CPU cores
- Enable gzip compression for API responses
- Optimize database queries with proper indexes

### Database
- PostgreSQL: Tune `shared_buffers`, `work_mem`, `max_connections`
- ClickHouse: Configure `max_memory_usage`, `max_threads`
- Redis: Set `maxmemory` and `maxmemory-policy`
- Regular VACUUM for PostgreSQL
- Partitioning for large tables

### Docker
- Use multi-stage builds to reduce image size
- Minimize layers in Dockerfiles
- Use `.dockerignore` to exclude unnecessary files
- Configure resource limits in docker-compose
- Use Docker BuildKit for faster builds
- Prune unused images and volumes regularly

## Monitoring and Logging

### Log Locations
- Backend: `./logs/backend/`
- Airflow: `./logs/airflow/`
- HDFS: `./logs/hdfs/`
- Redis: `./logs/redis/`
- Docker logs: `docker-compose logs <service>`

### Health Checks
All services expose health check endpoints:
- Backend: `http://localhost:8000/health`
- Airflow: `http://localhost:8080/health`
- ClickHouse: `http://localhost:8123/ping`
- HDFS: `http://localhost:9870/`
- Redis: `redis-cli ping`

### Monitoring Best Practices
- Monitor CPU/memory usage with `docker stats`
- Track API response times and error rates
- Monitor database connection pool usage
- Set up alerts for service failures
- Implement distributed tracing (OpenTelemetry)
- Aggregate logs (ELK stack, Grafana Loki)
- Monitor disk space for data directories

## Useful Resources

### Documentation
- FastAPI: https://fastapi.tiangolo.com
- SQLAlchemy: https://docs.sqlalchemy.org
- dlt: https://dlthub.com/docs
- Apache Airflow: https://airflow.apache.org/docs
- React: https://react.dev
- Docker Compose: https://docs.docker.com/compose

### Tools
- API Testing: Postman, HTTPie, curl
- WebSocket Testing: wscat, Postman
- Database Clients: DBeaver, pgAdmin, TablePlus
- Log Viewing: Docker Desktop, Portainer
- Performance: Apache Bench, Locust, K6

## Contributing

When modifying this codebase:
1. Follow all code standards documented above
2. Test changes locally with Docker Compose
3. Update this documentation if adding new features
4. Ensure all services pass health checks
5. Run linting and type checking
6. Add tests for new functionality
7. Review logs for errors before committing