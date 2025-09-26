.PHONY: help build start start-prod start-dev stop clean logs status test

# Default target
help:
	@echo "BigData Processing System - Docker Management"
	@echo "============================================="
	@echo "Available targets:"
	@echo "  build      - Build all Docker images"
	@echo "  start      - Start all services (production)"
	@echo "  start-prod - Start all services (production)"
	@echo "  start-dev  - Start all services (development)"
	@echo "  stop       - Stop all services"
	@echo "  clean      - Stop and remove all containers, volumes, and networks"
	@echo "  logs       - Show logs from all services"
	@echo "  status     - Show status of all services"
	@echo "  test       - Run system health checks"
	@echo "  airflow    - Show Airflow specific commands"

# Build all images
build:
	docker-compose -f docker-compose.prod.yml build

# Start production system
start: start-prod

start-prod:
	@echo "Starting production system..."
	@mkdir -p data logs/{backend,dlt-worker,airflow,postgres,redis,hdfs}
	docker-compose -f docker-compose.prod.yml up -d
	@echo "Waiting for services to be ready..."
	@sleep 30
	@make status

# Start development system
start-dev:
	@echo "Starting development system..."
	@mkdir -p data logs/{backend,dlt-worker,airflow,postgres,redis,hdfs}
	docker-compose up -d
	@echo "Waiting for services to be ready..."
	@sleep 30
	@make status

# Stop all services
stop:
	docker-compose -f docker-compose.prod.yml down
	docker-compose down

# Clean everything
clean:
	@echo "WARNING: This will remove all containers, volumes, and data!"
	@read -p "Are you sure? [y/N] " -n 1 -r && echo && \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose -f docker-compose.prod.yml down -v --remove-orphans; \
		docker-compose down -v --remove-orphans; \
		docker system prune -f; \
		rm -rf data logs; \
	fi

# Show logs
logs:
	docker-compose -f docker-compose.prod.yml logs -f

# Show service status
status:
	@echo "Production Services Status:"
	@docker-compose -f docker-compose.prod.yml ps
	@echo ""
	@echo "Development Services Status:"
	@docker-compose ps

# Run health checks
test:
	@echo "Running system health checks..."
	@echo "Backend API:"
	@curl -f http://localhost:8000/health || echo "Backend not ready"
	@echo "Frontend:"
	@curl -f http://localhost:3000 || echo "Frontend not ready"
	@echo "Airflow:"
	@curl -f http://localhost:8080/health || echo "Airflow not ready"
	@echo "ClickHouse:"
	@curl -f http://localhost:8123/ping || echo "ClickHouse not ready"
	@echo "HDFS:"
	@curl -f http://localhost:9870 || echo "HDFS not ready"

# Airflow management
airflow:
	@echo "Airflow Management Commands:"
	@echo "  docker-compose -f docker-compose.prod.yml exec airflow-webserver airflow dags list"
	@echo "  docker-compose -f docker-compose.prod.yml exec airflow-webserver airflow users list"
	@echo "  docker-compose -f docker-compose.prod.yml exec airflow-webserver airflow connections list"

# Database management
db-shell:
	docker-compose -f docker-compose.prod.yml exec postgres psql -U bigdata_user -d bigdata_db

# Backup data
backup:
	@echo "Creating backup..."
	@mkdir -p backups
	docker-compose -f docker-compose.prod.yml exec postgres pg_dump -U bigdata_user bigdata_db > backups/bigdata_db_$(shell date +%Y%m%d_%H%M%S).sql

# Show system resources
resources:
	@echo "Docker System Resources:"
	docker system df
	@echo ""
	@echo "Running Containers:"
	docker stats --no-stream