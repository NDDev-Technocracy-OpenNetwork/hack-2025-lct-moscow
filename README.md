# Система распределенной обработки больших данных

Микросервисная система для загрузки, анализа и обработки больших файлов данных (CSV, JSON, XML) с автоматической генерацией DDL/ETL скриптов и загрузкой в целевые хранилища (PostgreSQL, ClickHouse, HDFS).

## Архитектура

### Компоненты системы

- **Backend (FastAPI)** - REST API и WebSocket сервер для управления задачами обработки
- **Frontend (React)** - веб-интерфейс с визуализацией процесса обработки (React Flow)
- **PostgreSQL** - хранилище метаданных и целевая БД для загрузки данных
- **ClickHouse** - аналитическая СУБД для больших объемов данных
- **HDFS** - распределенное файловое хранилище (Hadoop)
- **Redis** - кэш и брокер сообщений для real-time обновлений
- **Nginx** - обратный прокси для маршрутизации запросов
- **Airflow** (опционально) - оркестрация ETL процессов

### Технологический стек

**Backend:**
- Python 3.11, FastAPI, SQLAlchemy 2.0, asyncpg
- Python Socket.IO для WebSocket коммуникации
- Pandas, lxml, xmltodict для обработки файлов
- PostgreSQL COPY для высокопроизводительной загрузки данных

**Frontend:**
- React 18, TypeScript, Vite
- Material-UI (MUI) для компонентов интерфейса
- React Flow для визуализации pipeline
- Socket.IO Client для real-time обновлений
- Zustand для управления состоянием

**Инфраструктура:**
- Docker, Docker Compose
- Nginx 1.25
- PostgreSQL 15
- ClickHouse 23.8
- Apache Hadoop 3.3 (HDFS)
- Redis 7

## Требования

- Docker 20.10+
- Docker Compose 2.0+
- 8GB RAM (минимум)
- 20GB свободного места на диске

## Установка и запуск

### 1. Клонирование репозитория

```bash
git clone <repository-url>
cd HACKATHON
```

### 2. Настройка переменных окружения

Скопируйте `.env.example` в `.env` и настройте параметры:

```bash
cp .env.example .env
```

Основные параметры в `.env`:

```env
# PostgreSQL
POSTGRES_USER=bigdata_user
POSTGRES_PASSWORD=bigdata_pass
POSTGRES_DB=bigdata_db

# ClickHouse
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=clickhouse_pass
CLICKHOUSE_DB=default

# Redis
REDIS_URL=redis://redis:6379/0

# Backend
DATABASE_URL=postgresql+asyncpg://bigdata_user:bigdata_pass@postgres:5432/bigdata_db
LOG_LEVEL=info
```

### 3. Запуск базовых сервисов

```bash
docker-compose up -d
```

Это запустит: backend, frontend, postgres, clickhouse, redis, namenode, datanode, nginx.

### 4. Запуск с Airflow (опционально)

```bash
docker-compose --profile airflow up -d
```

## Доступ к сервисам

- **Веб-интерфейс**: http://localhost (порт 80)
- **Backend API**: http://localhost/api/v1
- **API документация**: http://localhost/api/v1/docs
- **HDFS UI**: http://localhost:9870
- **ClickHouse HTTP**: http://localhost:8123
- **Airflow UI**: http://localhost:8080 (если запущен с профилем)

## Использование

### Загрузка файла через веб-интерфейс

1. Откройте http://localhost в браузере
2. Выберите файл (CSV, JSON, XML)
3. Укажите тип назначения (PostgreSQL, ClickHouse, HDFS)
4. Настройте параметры подключения
5. Нажмите "Upload" для начала обработки

### Мониторинг обработки

- Прогресс обработки отображается в реальном времени через WebSocket
- Доступна визуализация pipeline через "View Process Flow"
- DDL и ETL скрипты генерируются автоматически и доступны после завершения

### Работа с API

Примеры запросов:

```bash
# Получить список всех задач
curl http://localhost/api/v1/jobs

# Получить информацию о конкретной задаче
curl http://localhost/api/v1/jobs/{job_id}

# Получить DDL/ETL скрипты
curl http://localhost/api/v1/jobs/{job_id}/ddl

# Отменить задачу
curl -X DELETE http://localhost/api/v1/jobs/{job_id}
```

## Возможности

### Форматы файлов

- **CSV** - с автоопределением разделителя и кодировки
- **JSON** - массивы объектов и JSON Lines
- **XML** - с автоматическим парсингом структуры

### Целевые хранилища

- **PostgreSQL** - с автоматическим созданием таблиц и индексов
- **ClickHouse** - для аналитических запросов
- **HDFS** - для распределенного хранения

### Обработка данных

- Потоковая обработка файлов любого размера (chunked processing)
- Автоматическое определение схемы данных
- Генерация DDL скриптов для целевых БД
- Генерация ETL скриптов (dlt pipelines)
- Обработка специальных символов и некорректных данных
- Прогресс-бар с отображением обработанных записей

### Производительность

- Bulk insert через PostgreSQL COPY (TEXT format)
- Batch commits (каждые 5 chunks)
- Асинхронная архитектура (FastAPI + asyncpg)
- Chunked processing (10,000 записей на chunk)
- Оптимизированные Redis публикации

## Архитектура обработки

1. **File Upload** - загрузка файла на backend
2. **File Analysis** - определение формата, схемы, кодировки
3. **Schema Generation** - создание DDL и ETL скриптов
4. **Data Loading** - потоковая загрузка данных в целевое хранилище
5. **Completion** - финализация и сохранение метаданных

## Структура проекта

```
.
├── services/
│   ├── backend/          # FastAPI backend
│   │   ├── app/
│   │   │   ├── api/v1/  # REST endpoints
│   │   │   ├── core/    # Бизнес-логика
│   │   │   ├── models/  # SQLAlchemy модели
│   │   │   └── main.py  # Точка входа
│   │   └── requirements.txt
│   ├── frontend/         # React frontend
│   │   ├── src/
│   │   │   ├── pages/   # Страницы приложения
│   │   │   ├── store/   # Zustand state management
│   │   │   └── utils/   # API клиент, WebSocket
│   │   └── package.json
│   └── dlt-worker/      # ETL worker (опционально)
├── config/              # Конфигурации сервисов
│   ├── nginx/
│   ├── postgres/
│   ├── clickhouse/
│   └── hdfs/
├── data/                # Директория для данных
│   ├── input/          # Загруженные файлы
│   ├── temp/           # Временные файлы
│   └── processed/      # Обработанные файлы
├── logs/               # Логи сервисов
├── docker-compose.yml  # Оркестрация сервисов
├── .env               # Переменные окружения
└── README.md
```

## Команды для работы

### Управление сервисами

```bash
# Запуск всех сервисов
docker-compose up -d

# Остановка сервисов
docker-compose down

# Перезапуск конкретного сервиса
docker-compose restart backend

# Просмотр логов
docker-compose logs -f backend
docker-compose logs -f --tail 100 backend

# Просмотр статуса
docker-compose ps

# Пересборка образа
docker-compose build backend --no-cache
```

### Работа с базой данных

```bash
# Подключение к PostgreSQL
docker-compose exec postgres psql -U bigdata_user -d bigdata_db

# Подключение к ClickHouse
docker-compose exec clickhouse clickhouse-client

# Подключение к Redis
docker-compose exec redis redis-cli

# Бэкап PostgreSQL
docker-compose exec postgres pg_dump -U bigdata_user bigdata_db > backup.sql
```

### Работа с HDFS

```bash
# Список файлов в HDFS
docker-compose exec namenode hdfs dfs -ls /

# Создание директории
docker-compose exec namenode hdfs dfs -mkdir -p /data/input

# Загрузка файла в HDFS
docker-compose exec namenode hdfs dfs -put /local/path /hdfs/path
```

### Очистка

```bash
# Удалить все контейнеры и volumes (потеря данных!)
docker-compose down -v

# Очистить неиспользуемые Docker ресурсы
docker system prune -a --volumes
```

## Устранение неполадок

### Backend не запускается

Проверьте подключение к PostgreSQL:
```bash
docker-compose exec postgres pg_isready -U bigdata_user
docker-compose logs postgres
```

### WebSocket не подключается

1. Проверьте логи nginx: `docker-compose logs nginx`
2. Убедитесь, что backend запущен: `docker-compose ps backend`
3. Очистите кэш браузера (Ctrl+F5)

### HDFS datanode не регистрируется

```bash
# Проверьте namenode
docker-compose logs namenode

# Перезапустите datanode
docker-compose restart datanode
```

### Ошибки при обработке файлов

Проверьте логи backend:
```bash
docker-compose logs backend --tail 200 | grep ERROR
```

## Ограничения

- Максимальный размер файла: 10GB (настраивается в nginx.conf)
- Timeout обработки: 600 секунд (настраивается в gunicorn.conf.py)
- Формат CSV: требуется первая строка с заголовками
- XML: поддерживаются только плоские структуры (список элементов)

## Безопасность

**Важно для production:**

1. Измените пароли по умолчанию в `.env`
2. Настройте SSL/TLS для nginx
3. Ограничьте доступ к портам через firewall
4. Используйте secrets management (Vault, AWS Secrets Manager)
5. Регулярно обновляйте Docker образы
6. Настройте аутентификацию для API

## Производительность

Типичные показатели производительности:

- CSV (1GB, 1M записей): ~30-40 секунд
- JSON (500MB, 500K записей): ~25-35 секунд
- XML (1GB, 200K записей): ~40-50 секунд

Факторы, влияющие на скорость:
- Размер chunk (по умолчанию 10,000 записей)
- Частота commits (по умолчанию каждые 5 chunks)
- Целевое хранилище (PostgreSQL быстрее чем ClickHouse)
- Ресурсы Docker (CPU, RAM)

## Лицензия

MIT License

## Контакты

При возникновении проблем создайте issue в репозитории проекта.
