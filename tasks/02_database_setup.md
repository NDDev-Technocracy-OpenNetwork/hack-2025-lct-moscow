# Задача 2: Database Setup

## Цель
Настроить базы данных и создать необходимые схемы для хранения метаданных, состояния задач и данных пользователей.

## Технические требования
- PostgreSQL для метаданных системы и как один из destinations
- ClickHouse для аналитических данных
- Инициализация схем через Docker
- Миграции для изменения структуры БД

## Структура баз данных

### PostgreSQL Databases
1. **bigdata_db** - основная БД для метаданных
2. **airflow_db** - БД для Airflow
3. **user_data** - схема для пользовательских данных

### ClickHouse Databases
1. **analytics** - БД для аналитических данных
2. **system_metrics** - системные метрики

## Схемы PostgreSQL

### 1. Таблица задач обработки (processing_jobs)
```sql
CREATE TABLE processing_jobs (
    id SERIAL PRIMARY KEY,
    job_id UUID UNIQUE NOT NULL,
    filename VARCHAR(255) NOT NULL,
    file_size BIGINT NOT NULL,
    file_format VARCHAR(50) NOT NULL, -- 'csv', 'json', 'xml'
    destination_type VARCHAR(50) NOT NULL, -- 'postgresql', 'clickhouse', 'hdfs'
    destination_config JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending', -- 'pending', 'processing', 'completed', 'failed'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    records_processed BIGINT DEFAULT 0,
    records_total BIGINT,
    ddl_script TEXT,
    etl_script TEXT
);

CREATE INDEX idx_processing_jobs_status ON processing_jobs(status);
CREATE INDEX idx_processing_jobs_job_id ON processing_jobs(job_id);
```

### 2. Таблица прогресса обработки (processing_progress)
```sql
CREATE TABLE processing_progress (
    id SERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES processing_jobs(job_id),
    step_name VARCHAR(100) NOT NULL,
    step_status VARCHAR(50) NOT NULL, -- 'pending', 'in_progress', 'completed', 'failed'
    records_processed BIGINT DEFAULT 0,
    total_records BIGINT,
    progress_percentage DECIMAL(5,2) DEFAULT 0.00,
    step_details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_progress_job_id ON processing_progress(job_id);
```

### 3. Таблица конфигураций destinations (destination_configs)
```sql
CREATE TABLE destination_configs (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    type VARCHAR(50) NOT NULL, -- 'postgresql', 'clickhouse', 'hdfs'
    config JSONB NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Вставить стандартные конфигурации
INSERT INTO destination_configs (name, type, config) VALUES
('Default PostgreSQL', 'postgresql', '{
    "host": "postgres",
    "port": 5432,
    "database": "user_data",
    "schema": "public",
    "table_prefix": "imported_"
}'),
('Default ClickHouse', 'clickhouse', '{
    "host": "clickhouse",
    "port": 8123,
    "database": "analytics",
    "table_prefix": "analytics_"
}'),
('Default HDFS', 'hdfs', '{
    "namenode_url": "hdfs://namenode:9000",
    "base_path": "/bigdata",
    "file_format": "parquet",
    "compression": "snappy"
}');
```

### 4. Таблица файловых метаданных (file_metadata)
```sql
CREATE TABLE file_metadata (
    id SERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES processing_jobs(job_id),
    original_filename VARCHAR(255) NOT NULL,
    file_path VARCHAR(500) NOT NULL,
    file_size BIGINT NOT NULL,
    file_hash VARCHAR(64), -- SHA256 hash
    columns_detected JSONB, -- автоматически определенные колонки
    sample_data JSONB, -- примеры данных для предварительного просмотра
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 5. Schema для пользовательских данных
```sql
CREATE SCHEMA IF NOT EXISTS user_data;
-- Здесь будут создаваться таблицы с импортированными данными
```

## ClickHouse Schemas

### 1. Таблица для аналитических данных (по умолчанию создается динамически)
```sql
-- Пример структуры будет создаваться динамически на основе входных данных
-- CREATE TABLE analytics.sample_data (
--     id UInt64,
--     timestamp DateTime,
--     data String,
--     metrics Nested(
--         name String,
--         value Float64
--     )
-- ) ENGINE = MergeTree()
-- ORDER BY (timestamp, id);
```

### 2. Таблица метрик системы
```sql
CREATE TABLE system_metrics.processing_stats (
    job_id String,
    timestamp DateTime,
    records_per_second UInt64,
    memory_usage_mb UInt64,
    cpu_usage_percent Float32,
    error_count UInt32
) ENGINE = MergeTree()
ORDER BY (timestamp, job_id);
```

## Конфигурационные файлы

### PostgreSQL init script (config/postgres/init.sql)
```sql
-- Создание пользователей
CREATE USER bigdata_user WITH PASSWORD 'bigdata_password';
CREATE USER airflow_user WITH PASSWORD 'airflow_password';

-- Создание баз данных
CREATE DATABASE bigdata_db OWNER bigdata_user;
CREATE DATABASE airflow_db OWNER airflow_user;

-- Подключение к bigdata_db для создания схем
\c bigdata_db;

-- Здесь выполняются все CREATE TABLE statements сверху
-- ... (весь код создания таблиц)

-- Права доступа
GRANT ALL PRIVILEGES ON DATABASE bigdata_db TO bigdata_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO bigdata_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO bigdata_user;
GRANT ALL PRIVILEGES ON SCHEMA user_data TO bigdata_user;

-- Подключение к airflow_db
\c airflow_db;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
```

### ClickHouse config (config/clickhouse/init.sql)
```sql
-- Создание баз данных
CREATE DATABASE IF NOT EXISTS analytics;
CREATE DATABASE IF NOT EXISTS system_metrics;

-- Создание пользователя для приложения
CREATE USER IF NOT EXISTS bigdata_user IDENTIFIED WITH plaintext_password BY 'bigdata_password';
GRANT ALL ON analytics.* TO bigdata_user;
GRANT ALL ON system_metrics.* TO bigdata_user;

-- Создание системных таблиц
CREATE TABLE IF NOT EXISTS system_metrics.processing_stats (
    job_id String,
    timestamp DateTime,
    records_per_second UInt64,
    memory_usage_mb UInt64,
    cpu_usage_percent Float32,
    error_count UInt32
) ENGINE = MergeTree()
ORDER BY (timestamp, job_id);
```

### ClickHouse configuration (config/clickhouse/config.xml)
```xml
<?xml version="1.0"?>
<yandex>
    <logger>
        <level>information</level>
        <console>1</console>
    </logger>

    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>

    <listen_host>::</listen_host>
    <listen_host>0.0.0.0</listen_host>

    <max_connections>200</max_connections>
    <keep_alive_timeout>3</keep_alive_timeout>
    <max_concurrent_queries>100</max_concurrent_queries>

    <path>/var/lib/clickhouse/</path>
    <tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>

    <users>
        <default>
            <password></password>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </default>
        <bigdata_user>
            <password>bigdata_password</password>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </bigdata_user>
    </users>

    <profiles>
        <default>
            <max_memory_usage>10000000000</max_memory_usage>
            <use_uncompressed_cache>0</use_uncompressed_cache>
            <load_balancing>random</load_balancing>
        </default>
    </profiles>

    <quotas>
        <default>
            <interval>
                <duration>3600</duration>
                <queries>0</queries>
                <errors>0</errors>
                <result_rows>0</result_rows>
                <read_rows>0</read_rows>
                <execution_time>0</execution_time>
            </interval>
        </default>
    </quotas>
</yandex>
```

## Задачи для выполнения

### 1. Подготовить PostgreSQL
- [ ] Создать init.sql с созданием БД и пользователей
- [ ] Создать все необходимые таблицы для метаданных
- [ ] Настроить права доступа
- [ ] Добавить индексы для оптимизации

### 2. Подготовить ClickHouse
- [ ] Создать конфигурационный файл config.xml
- [ ] Создать init скрипт для БД и пользователей
- [ ] Настроить таблицы для аналитики
- [ ] Оптимизировать настройки для больших данных

### 3. Обновить docker-compose.yml
- [ ] Добавить монтирование init скриптов
- [ ] Настроить переменные окружения для БД
- [ ] Добавить health checks для БД
- [ ] Настроить зависимости между сервисами

### 4. Создать утилиты для работы с БД
- [ ] Скрипт для проверки подключений к БД
- [ ] Скрипт для создания миграций
- [ ] Утилита для очистки тестовых данных

### 5. Протестировать настройку
- [ ] Проверить создание всех БД и таблиц
- [ ] Убедиться в корректности подключений
- [ ] Протестировать вставку и выборку данных
- [ ] Проверить права доступа пользователей

## Ожидаемый результат
После выполнения этой задачи должны быть готовы все базы данных с правильными схемами, пользователями и правами доступа.

## Следующая задача
[03_airflow_configuration.md](./03_airflow_configuration.md) - Настройка Apache Airflow