# BigData Processing System MVP

A microservices-based system for processing large files (up to 10GB) with real-time visualization and multiple destination support.

## Architecture

- **Frontend**: React.js with React Flow for process visualization
- **Backend**: FastAPI with WebSocket support
- **ETL Worker**: dlt library for data processing
- **Orchestration**: Apache Airflow
- **Destinations**: PostgreSQL, ClickHouse, HDFS
- **Infrastructure**: Docker & Docker Compose

## Features

- 🚀 Process files up to 10GB (CSV, JSON, XML)
- 📊 Real-time visualization with React Flow
- 🔄 WebSocket real-time updates
- 🎯 Multiple destination support
- ⚡ Stream processing for large files
- 🔧 Dynamic DDL/ETL script generation

## Quick Start

1. **Clone and navigate to the project**
   ```bash
   cd HACKATHON
   ```

2. **Start all services**
   ```bash
   docker-compose up -d
   ```

3. **Access the services**
   - Frontend: http://localhost:3000
   - Backend API: http://localhost:8000
   - Airflow: http://localhost:8080 (admin/admin123)
   - ClickHouse: http://localhost:8123
   - HDFS: http://localhost:9870

## Service Status Check

```bash
# Check all services
docker-compose ps

# View logs
docker-compose logs -f [service_name]

# Health check
curl http://localhost:8000/health
```

## Development Status

- ✅ Docker infrastructure setup
- ⏳ Database schemas configuration
- ⏳ FastAPI backend development
- ⏳ dlt worker implementation
- ⏳ React UI development
- ⏳ Airflow DAG setup
- ⏳ Integration testing

## File Processing

1. Upload file through web interface
2. Select destination (PostgreSQL/ClickHouse/HDFS)
3. Configure destination settings
4. Monitor real-time progress
5. View generated DDL/ETL scripts

## Project Structure

```
├── docker-compose.yml          # Main orchestration
├── services/
│   ├── frontend/              # React application
│   ├── backend/               # FastAPI service
│   ├── dlt-worker/           # ETL processing
│   └── airflow/              # Workflow orchestration
├── config/                    # Service configurations
├── data/                      # File processing directories
└── tasks/                     # Detailed implementation tasks
```

## Next Steps

Follow the task sequence in the `tasks/` directory:
1. Complete database setup
2. Implement FastAPI backend
3. Build React frontend
4. Configure Airflow integration
5. Test with large files

## Support

For detailed implementation guidance, see the task files in `tasks/01_docker_infrastructure.md` through `tasks/12_deployment_optimization.md`.