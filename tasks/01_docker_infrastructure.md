# Задача 1: Docker Infrastructure Setup

## Цель
Настроить базовую Docker инфраструктуру для микросервисной архитектуры системы обработки больших данных.

## Технические требования
- Docker Compose для оркестрации контейнеров
- Отдельные контейнеры для каждого сервиса
- Shared volumes для обмена файлами
- Сеть для взаимодействия сервисов
- Health checks для всех сервисов

## Структура проекта
```
project/
├── docker-compose.yml
├── services/
│   ├── frontend/
│   │   └── Dockerfile
│   ├── backend/
│   │   └── Dockerfile
│   ├── dlt-worker/
│   │   └── Dockerfile
│   └── airflow/
│       ├── Dockerfile
│       └── dags/
├── data/
│   ├── input/          # Входящие файлы
│   ├── temp/           # Временные файлы
│   └── processed/      # Обработанные файлы
└── config/
    ├── postgres/
    ├── clickhouse/
    └── hdfs/
```

## Сервисы в docker-compose.yml

### 1. Frontend (React)
```yaml
frontend:
  build: ./services/frontend
  ports:
    - "3000:3000"
  environment:
    - REACT_APP_API_URL=http://backend:8000
  depends_on:
    - backend
```

### 2. Backend API (FastAPI)
```yaml
backend:
  build: ./services/backend
  ports:
    - "8000:8000"
  environment:
    - DATABASE_URL=postgresql://user:pass@postgres:5432/bigdata_db
    - REDIS_URL=redis://redis:6379
  depends_on:
    - postgres
    - redis
  volumes:
    - ./data:/app/data
```

### 3. dlt Worker
```yaml
dlt-worker:
  build: ./services/dlt-worker
  environment:
    - DATABASE_URL=postgresql://user:pass@postgres:5432/bigdata_db
    - CLICKHOUSE_URL=clickhouse://clickhouse:8123/default
    - HDFS_NAMENODE_URL=hdfs://namenode:9000
  depends_on:
    - postgres
    - clickhouse
    - namenode
  volumes:
    - ./data:/app/data
```

### 4. PostgreSQL (метаданные + destination)
```yaml
postgres:
  image: postgres:15
  environment:
    - POSTGRES_DB=bigdata_db
    - POSTGRES_USER=user
    - POSTGRES_PASSWORD=password
  ports:
    - "5432:5432"
  volumes:
    - postgres_data:/var/lib/postgresql/data
    - ./config/postgres/init.sql:/docker-entrypoint-initdb.d/init.sql
```

### 5. ClickHouse (аналитическое хранилище)
```yaml
clickhouse:
  image: clickhouse/clickhouse-server:23.8
  ports:
    - "8123:8123"
    - "9000:9000"
  environment:
    - CLICKHOUSE_DB=analytics
    - CLICKHOUSE_USER=user
    - CLICKHOUSE_PASSWORD=password
  volumes:
    - clickhouse_data:/var/lib/clickhouse
    - ./config/clickhouse/config.xml:/etc/clickhouse-server/config.xml
```

### 6. HDFS (Hadoop Distributed File System)
```yaml
namenode:
  image: apache/hadoop:3.3.4
  command: ["hdfs", "namenode"]
  ports:
    - "9870:9870"
    - "9000:9000"
  environment:
    - HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
  volumes:
    - namenode_data:/hadoop/dfs/name
    - ./config/hdfs/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml
    - ./config/hdfs/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml

datanode:
  image: apache/hadoop:3.3.4
  command: ["hdfs", "datanode"]
  environment:
    - HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
  depends_on:
    - namenode
  volumes:
    - datanode_data:/hadoop/dfs/data
    - ./config/hdfs/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml
    - ./config/hdfs/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml
```

### 7. Airflow
```yaml
airflow-webserver:
  build: ./services/airflow
  command: webserver
  ports:
    - "8080:8080"
  environment:
    - AIRFLOW__CORE__EXECUTOR=LocalExecutor
    - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://user:pass@postgres:5432/airflow_db
  depends_on:
    - postgres
  volumes:
    - ./services/airflow/dags:/opt/airflow/dags

airflow-scheduler:
  build: ./services/airflow
  command: scheduler
  depends_on:
    - postgres
    - airflow-webserver
  volumes:
    - ./services/airflow/dags:/opt/airflow/dags
```

### 8. Redis (для кеширования и очередей)
```yaml
redis:
  image: redis:7-alpine
  ports:
    - "6379:6379"
  volumes:
    - redis_data:/data
```

## Volumes
```yaml
volumes:
  postgres_data:
  clickhouse_data:
  namenode_data:
  datanode_data:
  redis_data:
```

## Сети
```yaml
networks:
  default:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.0.0/16
```

## Задачи для выполнения

### 1. Создать основной docker-compose.yml
- [ ] Определить все сервисы
- [ ] Настроить зависимости между сервисами
- [ ] Добавить health checks
- [ ] Настроить volumes и сети

### 2. Создать базовые Dockerfile для каждого сервиса
- [ ] Frontend Dockerfile (Node.js + React)
- [ ] Backend Dockerfile (Python + FastAPI)
- [ ] dlt-worker Dockerfile (Python + dlt)
- [ ] Airflow Dockerfile (с кастомными DAG'ами)

### 3. Подготовить конфигурационные файлы
- [ ] PostgreSQL init scripts
- [ ] ClickHouse конфигурация
- [ ] HDFS конфигурация (core-site.xml, hdfs-site.xml)
- [ ] Airflow конфигурация

### 4. Создать структуру директорий
- [ ] Создать все необходимые папки
- [ ] Настроить права доступа для shared volumes
- [ ] Добавить .gitignore для временных файлов

### 5. Протестировать базовый запуск
- [ ] `docker-compose up -d` должен запустить все сервисы
- [ ] Проверить доступность всех портов
- [ ] Убедиться, что сервисы видят друг друга
- [ ] Проверить health checks

## Ожидаемый результат
После выполнения этой задачи должна быть готова базовая Docker инфраструктура, где все сервисы запускаются и могут взаимодействовать друг с другом.

## Следующая задача
[02_database_setup.md](./02_database_setup.md) - Настройка баз данных и схем