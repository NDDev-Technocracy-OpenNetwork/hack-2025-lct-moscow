# Задача 3: Airflow Configuration

## Цель
Настроить Apache Airflow для оркестрации ETL процессов с поддержкой динамического создания DAG'ов для обработки загруженных файлов.

## Технические требования
- Airflow 2.7+ с LocalExecutor
- Динамическое создание DAG'ов на основе запросов пользователей
- Интеграция с dlt для ETL процессов
- REST API для управления задачами
- WebSocket уведомления о статусе выполнения

## Структура Airflow

### Директории
```
services/airflow/
├── Dockerfile
├── requirements.txt
├── dags/
│   ├── dynamic_etl_dag.py
│   ├── templates/
│   │   ├── csv_processing_template.py
│   │   ├── json_processing_template.py
│   │   └── xml_processing_template.py
│   └── utils/
│       ├── dlt_operators.py
│       ├── notification_operators.py
│       └── file_validators.py
├── plugins/
│   ├── custom_operators/
│   └── custom_hooks/
└── config/
    └── airflow.cfg
```

## Dockerfile для Airflow

```dockerfile
FROM apache/airflow:2.7.1-python3.11

USER root

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Копирование requirements
COPY requirements.txt .

# Установка Python зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Копирование DAG'ов и конфигурации
COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/
COPY config/airflow.cfg /opt/airflow/airflow.cfg

# Установка переменных окружения
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
```

## Requirements для Airflow

```txt
# requirements.txt
apache-airflow==2.7.1
apache-airflow-providers-postgres==5.6.0
apache-airflow-providers-http==4.5.1
dlt[postgresql,clickhouse,s3]>=1.17.0
pandas>=2.0.0
pyarrow>=16.0.0
requests>=2.31.0
websockets>=11.0
psycopg2-binary>=2.9.7
clickhouse-driver>=0.2.6
hdfs3>=0.3.1
```

## Основной динамический DAG

### dynamic_etl_dag.py
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dags.utils.dlt_operators import DltETLOperator
from dags.utils.notification_operators import WebSocketNotifyOperator
from dags.utils.file_validators import FileValidatorOperator

def create_dynamic_etl_dag(job_config):
    """
    Создает динамический DAG для обработки файла

    Args:
        job_config: dict с конфигурацией задачи
            - job_id: UUID задачи
            - filename: имя файла
            - file_format: формат файла (csv, json, xml)
            - destination_type: тип назначения (postgresql, clickhouse, hdfs)
            - destination_config: конфигурация назначения
    """

    dag_id = f"etl_job_{job_config['job_id']}"

    default_args = {
        'owner': 'bigdata-system',
        'depends_on_past': False,
        'start_date': datetime.now(),
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': notify_failure,
        'on_success_callback': notify_success,
    }

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f'ETL process for {job_config["filename"]}',
        schedule_interval=None,  # Запускается только по требованию
        max_active_runs=1,
        catchup=False,
        tags=['etl', 'dynamic', job_config['file_format']],
    )

    # Task 1: Валидация файла
    validate_file = FileValidatorOperator(
        task_id='validate_file',
        job_config=job_config,
        dag=dag
    )

    # Task 2: Анализ структуры файла
    @task(dag=dag)
    def analyze_file_structure(**context):
        """Анализирует структуру файла и создает DDL"""
        from dags.utils.file_analyzer import analyze_file, generate_ddl

        job_config = context['dag_run'].conf
        file_path = f"/app/data/input/{job_config['filename']}"

        # Анализ файла
        schema_info = analyze_file(file_path, job_config['file_format'])

        # Генерация DDL
        ddl_script = generate_ddl(schema_info, job_config['destination_type'], job_config['destination_config'])

        # Сохранение в метаданные
        pg_hook = PostgresHook(postgres_conn_id='bigdata_postgres')
        pg_hook.run(
            "UPDATE processing_jobs SET ddl_script = %s WHERE job_id = %s",
            parameters=[ddl_script, job_config['job_id']]
        )

        return schema_info

    analyze_structure = analyze_file_structure()

    # Task 3: Обработка данных через dlt
    process_data = DltETLOperator(
        task_id='process_with_dlt',
        job_config=job_config,
        dag=dag
    )

    # Task 4: Уведомление о прогрессе
    notify_progress = WebSocketNotifyOperator(
        task_id='notify_progress',
        job_config=job_config,
        message_type='progress_update',
        dag=dag
    )

    # Task 5: Финальная проверка и уведомление
    @task(dag=dag)
    def finalize_job(**context):
        """Финализация задачи и обновление статуса"""
        job_config = context['dag_run'].conf

        # Подсчет обработанных записей
        # Обновление статуса в БД
        pg_hook = PostgresHook(postgres_conn_id='bigdata_postgres')
        pg_hook.run(
            """UPDATE processing_jobs
               SET status = 'completed', completed_at = CURRENT_TIMESTAMP
               WHERE job_id = %s""",
            parameters=[job_config['job_id']]
        )

        return "Job completed successfully"

    finalize = finalize_job()

    # Определение зависимостей между задачами
    validate_file >> analyze_structure >> process_data >> notify_progress >> finalize

    return dag

def notify_failure(context):
    """Callback для уведомления об ошибке"""
    job_id = context['dag_run'].conf.get('job_id')
    error_msg = str(context['exception'])

    # Обновление статуса в БД
    pg_hook = PostgresHook(postgres_conn_id='bigdata_postgres')
    pg_hook.run(
        """UPDATE processing_jobs
           SET status = 'failed', error_message = %s
           WHERE job_id = %s""",
        parameters=[error_msg, job_id]
    )

def notify_success(context):
    """Callback для уведомления об успехе"""
    job_id = context['dag_run'].conf.get('job_id')

    # WebSocket уведомление о завершении
    # Будет реализовано в WebSocketNotifyOperator

# Регистрация DAG'ов происходит динамически через API
```

## Кастомные операторы

### dlt_operators.py
```python
from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import dlt
import pandas as pd
from typing import Dict, Any

class DltETLOperator(BaseOperator):
    """
    Оператор для выполнения ETL процессов с помощью dlt
    """

    def __init__(self, job_config: Dict[str, Any], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_config = job_config

    def execute(self, context):
        """Выполнение ETL процесса"""

        # Настройка dlt pipeline в зависимости от destination
        destination_type = self.job_config['destination_type']

        if destination_type == 'postgresql':
            pipeline = dlt.pipeline(
                pipeline_name=f"job_{self.job_config['job_id']}",
                destination='postgres',
                dataset_name=self.job_config['destination_config'].get('schema', 'public')
            )
        elif destination_type == 'clickhouse':
            pipeline = dlt.pipeline(
                pipeline_name=f"job_{self.job_config['job_id']}",
                destination='clickhouse',
                dataset_name=self.job_config['destination_config'].get('database', 'analytics')
            )
        elif destination_type == 'hdfs':
            pipeline = dlt.pipeline(
                pipeline_name=f"job_{self.job_config['job_id']}",
                destination='filesystem',
                dataset_name='hdfs_data'
            )

        # Загрузка и обработка файла
        file_path = f"/app/data/input/{self.job_config['filename']}"

        @dlt.resource
        def load_file():
            if self.job_config['file_format'] == 'csv':
                return pd.read_csv(file_path, chunksize=10000)  # Чтение частями для больших файлов
            elif self.job_config['file_format'] == 'json':
                return pd.read_json(file_path, lines=True, chunksize=10000)
            elif self.job_config['file_format'] == 'xml':
                return pd.read_xml(file_path, chunksize=10000)

        # Запуск pipeline
        load_info = pipeline.run(load_file())

        # Обновление прогресса в БД
        self._update_progress(load_info)

        return load_info

    def _update_progress(self, load_info):
        """Обновление прогресса в базе данных"""
        pg_hook = PostgresHook(postgres_conn_id='bigdata_postgres')

        # Подсчет обработанных записей
        records_processed = sum(table.jobs[0].job.recordsLoaded for table in load_info.load_packages[0].jobs)

        pg_hook.run(
            """UPDATE processing_jobs
               SET records_processed = %s, status = 'processing'
               WHERE job_id = %s""",
            parameters=[records_processed, self.job_config['job_id']]
        )
```

### notification_operators.py
```python
from airflow.models.baseoperator import BaseOperator
import requests
import json

class WebSocketNotifyOperator(BaseOperator):
    """
    Оператор для отправки WebSocket уведомлений
    """

    def __init__(self, job_config, message_type, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_config = job_config
        self.message_type = message_type

    def execute(self, context):
        """Отправка уведомления через backend API"""

        message = {
            'job_id': self.job_config['job_id'],
            'type': self.message_type,
            'timestamp': context['ts'],
            'task_id': context['task_instance'].task_id,
            'status': 'success' if context.get('task_instance').state == 'success' else 'running'
        }

        # Отправка на backend для рассылки через WebSocket
        try:
            response = requests.post(
                'http://backend:8000/api/v1/notify',
                json=message,
                timeout=10
            )
            response.raise_for_status()
        except Exception as e:
            self.log.error(f"Failed to send notification: {e}")
            # Не падаем, если уведомление не отправилось

        return message
```

## Конфигурация Airflow

### airflow.cfg (основные настройки)
```ini
[core]
dags_folder = /opt/airflow/dags
hostname_callable = airflow.utils.net.getfqdn
default_timezone = utc
executor = LocalExecutor
parallelism = 32
dag_concurrency = 16
dags_are_paused_at_creation = False
max_active_runs_per_dag = 1
load_examples = False
enable_xcom_pickling = True

[database]
sql_alchemy_conn = postgresql://airflow_user:airflow_password@postgres:5432/airflow_db
sql_alchemy_pool_size = 10
sql_alchemy_max_overflow = 20
sql_alchemy_pool_recycle = 3600

[webserver]
base_url = http://localhost:8080
web_server_port = 8080
web_server_host = 0.0.0.0
expose_config = True
authenticate = False
rbac = False

[api]
auth_backends = airflow.api.auth.backend.default

[logging]
base_log_folder = /opt/airflow/logs
logging_level = INFO
fab_logging_level = INFO
```

## Задачи для выполнения

### 1. Подготовить Dockerfile и зависимости
- [ ] Создать Dockerfile с установкой dlt и других зависимостей
- [ ] Подготовить requirements.txt со всеми необходимыми пакетами
- [ ] Настроить права доступа в контейнере

### 2. Создать базовую структуру DAG'ов
- [ ] Реализовать динамический DAG для ETL процессов
- [ ] Создать шаблоны для разных типов файлов
- [ ] Добавить утилиты для работы с файлами

### 3. Разработать кастомные операторы
- [ ] DltETLOperator для интеграции с dlt
- [ ] WebSocketNotifyOperator для уведомлений
- [ ] FileValidatorOperator для валидации файлов

### 4. Настроить конфигурацию Airflow
- [ ] Подготовить airflow.cfg с оптимальными настройками
- [ ] Настроить подключения к базам данных
- [ ] Добавить переменные окружения

### 5. Интеграция с системой
- [ ] API для создания динамических DAG'ов
- [ ] Обработка callback'ов для уведомлений
- [ ] Логирование и мониторинг выполнения

### 6. Тестирование
- [ ] Проверить запуск Airflow webserver и scheduler
- [ ] Протестировать создание и выполнение DAG'ов
- [ ] Убедиться в корректности интеграции с dlt

## Ожидаемый результат
После выполнения этой задачи должен быть готов Airflow с поддержкой динамических DAG'ов для обработки загруженных файлов через dlt.

## Следующая задача
[04_fastapi_backend.md](./04_fastapi_backend.md) - Разработка Backend API