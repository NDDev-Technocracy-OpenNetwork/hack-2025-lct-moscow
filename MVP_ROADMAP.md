# MVP BigData Processing System - Roadmap

## Общий обзор системы

**Цель:** Создать микросервисную систему для обработки больших файлов (до 10GB) с веб-интерфейсом, позволяющую загружать данные в различные хранилища (PostgreSQL, ClickHouse, HDFS, S3) с использованием dlt, Airflow и визуализацией процесса через React Flow.

## Архитектура системы

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   React UI      │────│   Backend API    │────│   dlt Worker    │
│   (Frontend)    │    │   (FastAPI)      │    │   (Processing)  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                               │
                       ┌───────┼───────┐
                       │               │
                ┌──────────────┐ ┌─────────────┐
                │   Airflow    │ │ PostgreSQL  │
                │   (Orchestr.)│ │ (Metadata)  │
                └──────────────┘ └─────────────┘
                       │
        ┌──────────────┼──────────────┐
        │              │              │
  ┌───────────┐ ┌─────────────┐ ┌──────────┐
  │PostgreSQL │ │ ClickHouse  │ │   HDFS   │
  │(Storage)  │ │ (Storage)   │ │(Storage) │
  └───────────┘ └─────────────┘ └──────────┘
```

## Последовательность выполнения задач

### Фаза 1: Инфраструктура и базовая настройка
- [01_docker_infrastructure.md](./tasks/01_docker_infrastructure.md)
- [02_database_setup.md](./tasks/02_database_setup.md)
- [03_airflow_configuration.md](./tasks/03_airflow_configuration.md)

### Фаза 2: Backend разработка
- [04_fastapi_backend.md](./tasks/04_fastapi_backend.md)
- [05_dlt_integration.md](./tasks/05_dlt_integration.md)
- [06_websocket_realtime.md](./tasks/06_websocket_realtime.md)

### Фаза 3: Frontend разработка
- [07_react_ui_setup.md](./tasks/07_react_ui_setup.md)
- [08_file_upload_interface.md](./tasks/08_file_upload_interface.md)
- [09_react_flow_visualization.md](./tasks/09_react_flow_visualization.md)

### Фаза 4: Интеграция и тестирование
- [10_airflow_dlt_integration.md](./tasks/10_airflow_dlt_integration.md)
- [11_testing_big_files.md](./tasks/11_testing_big_files.md)
- [12_deployment_optimization.md](./tasks/12_deployment_optimization.md)

## Технологический стек

### Backend
- **API Server:** FastAPI (Python)
- **ETL Library:** dlt (для извлечения и загрузки данных)
- **Orchestration:** Apache Airflow
- **WebSockets:** FastAPI WebSocket support
- **Metadata Storage:** PostgreSQL

### Frontend
- **UI Framework:** React.js
- **Process Visualization:** React Flow
- **State Management:** Redux Toolkit или Zustand
- **HTTP Client:** Axios
- **WebSocket Client:** Socket.IO client

### Infrastructure
- **Containerization:** Docker & Docker Compose
- **Storage Destinations:**
  - PostgreSQL (реляционные данные)
  - ClickHouse (аналитические данные)
  - HDFS (большие файлы, Parquet формат)
  - S3 (временное хранение)

### Data Processing
- **File Formats:** CSV, JSON, XML (на входе)
- **Output Formats:** PostgreSQL tables, ClickHouse tables, Parquet (HDFS)
- **File Size:** До 10GB на файл
- **Processing:** Streaming обработка через dlt

## Основные возможности MVP

1. **Загрузка файлов** через веб-интерфейс
2. **Выбор destination** (PostgreSQL/ClickHouse/HDFS)
3. **Настройка конфигурации** для каждого destination
4. **Генерация DDL/ETL скриптов** и показ пользователю
5. **Real-time визуализация процесса** через React Flow
6. **Мониторинг прогресса** с метриками (количество записей)
7. **WebSocket обновления** статуса обработки
8. **Retry механизм** через Airflow при ошибках

## Критерии готовности MVP

- [ ] Успешная загрузка и обработка CSV файла 1GB+
- [ ] Работающая загрузка во все 3 destination
- [ ] Функционирующая визуализация процесса
- [ ] Real-time обновления статуса
- [ ] Генерация и показ DDL/ETL скриптов
- [ ] Базовая обработка ошибок и retry логика