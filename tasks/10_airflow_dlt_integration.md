# Задача 10: Airflow DLT Integration

## Цель
Полная интеграция Apache Airflow с dlt worker для создания полнофункциональных ETL pipelines с мониторингом, error handling и retry логикой.

## Технические требования
- Динамическое создание DAG'ов через API
- Интеграция с dlt worker для выполнения ETL
- Callback система для уведомлений
- Error handling и retry механизмы
- Мониторинг производительности

## Доработка Airflow DAG'ов

### dags/dynamic_etl_dag_factory.py
```python
import os
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

class ETLDAGFactory:
    """Фабрика для создания динамических ETL DAG'ов"""

    def __init__(self):
        self.default_args = {
            'owner': 'bigdata-system',
            'depends_on_past': False,
            'retries': 3,
            'retry_delay': timedelta(minutes=5),
            'email_on_failure': False,
            'email_on_retry': False,
            'start_date': datetime(2024, 1, 1),
        }

    def create_etl_dag(self, job_config: Dict[str, Any]) -> DAG:
        """Создание DAG для ETL процесса"""

        job_id = job_config['job_id']
        dag_id = f"etl_job_{job_id}"

        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            description=f'ETL process for {job_config.get("filename", "unknown")}',
            schedule_interval=None,
            max_active_runs=1,
            catchup=False,
            tags=['etl', 'dynamic', job_config.get('file_format', 'unknown')],
            params={
                'job_id': job_id,
                'job_config': json.dumps(job_config)
            }
        )

        # Task 1: Валидация и подготовка
        validate_task = PythonOperator(
            task_id='validate_and_prepare',
            python_callable=self._validate_and_prepare,
            op_kwargs={'job_config': job_config},
            dag=dag
        )

        # Task 2: Анализ файла и генерация DDL
        analyze_task = PythonOperator(
            task_id='analyze_file_and_generate_ddl',
            python_callable=self._analyze_file_and_generate_ddl,
            op_kwargs={'job_config': job_config},
            dag=dag
        )

        # Task 3: Создание схемы в destination
        create_schema_task = PythonOperator(
            task_id='create_destination_schema',
            python_callable=self._create_destination_schema,
            op_kwargs={'job_config': job_config},
            dag=dag
        )

        # Task 4: Запуск dlt worker
        dlt_processing_task = BashOperator(
            task_id='run_dlt_processing',
            bash_command=self._generate_dlt_command(job_config),
            env={
                'DLT_JOB_CONFIG': json.dumps(job_config),
                'PYTHONPATH': '/app',
            },
            dag=dag
        )

        # Task 5: Валидация результатов
        validate_results_task = PythonOperator(
            task_id='validate_results',
            python_callable=self._validate_results,
            op_kwargs={'job_config': job_config},
            dag=dag
        )

        # Task 6: Уведомление о завершении
        notify_completion_task = SimpleHttpOperator(
            task_id='notify_completion',
            http_conn_id='backend_api',
            endpoint='/api/v1/notify',
            method='POST',
            data=json.dumps({
                'job_id': job_id,
                'type': 'job_completed',
                'status': 'success'
            }),
            headers={'Content-Type': 'application/json'},
            dag=dag
        )

        # Task 7: Cleanup (при ошибках)
        cleanup_task = PythonOperator(
            task_id='cleanup_on_failure',
            python_callable=self._cleanup_on_failure,
            op_kwargs={'job_config': job_config},
            trigger_rule='one_failed',
            dag=dag
        )

        # Определение зависимостей
        validate_task >> analyze_task >> create_schema_task >> dlt_processing_task
        dlt_processing_task >> validate_results_task >> notify_completion_task
        [validate_task, analyze_task, create_schema_task, dlt_processing_task] >> cleanup_task

        return dag

    def _validate_and_prepare(self, job_config: Dict[str, Any], **context):
        """Валидация входных данных и подготовка окружения"""

        job_id = job_config['job_id']
        filename = job_config['filename']

        logger.info(f"Starting validation for job {job_id}")

        # Проверка существования файла
        file_path = f"/app/data/input/{filename}"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Input file not found: {file_path}")

        # Проверка размера файла
        file_size = os.path.getsize(file_path)
        if file_size > 10 * 1024 * 1024 * 1024:  # 10GB
            raise ValueError(f"File too large: {file_size} bytes")

        # Обновление статуса в БД
        self._update_job_status(job_id, 'processing', 'File validation completed')

        # Отправка уведомления
        self._send_progress_notification(job_id, 'validation', {
            'status': 'completed',
            'file_size': file_size,
            'file_path': file_path
        })

        return {
            'job_id': job_id,
            'file_path': file_path,
            'file_size': file_size,
            'validation_status': 'success'
        }

    def _analyze_file_and_generate_ddl(self, job_config: Dict[str, Any], **context):
        """Анализ структуры файла и генерация DDL скрипта"""

        job_id = job_config['job_id']
        file_format = job_config['file_format']
        destination_type = job_config['destination_type']
        destination_config = job_config['destination_config']

        logger.info(f"Analyzing file structure for job {job_id}")

        try:
            # Импорт модулей для анализа
            from dags.utils.file_analyzer import FileAnalyzer
            from dags.utils.ddl_generator import DDLGenerator

            # Анализ файла
            analyzer = FileAnalyzer()
            schema_info = analyzer.analyze_file(
                f"/app/data/input/{job_config['filename']}",
                file_format
            )

            # Генерация DDL скрипта
            ddl_generator = DDLGenerator()
            ddl_script = ddl_generator.generate_ddl(
                schema_info,
                destination_type,
                destination_config
            )

            # Сохранение DDL в БД
            pg_hook = PostgresHook(postgres_conn_id='bigdata_postgres')
            pg_hook.run(
                "UPDATE processing_jobs SET ddl_script = %s, records_total = %s WHERE job_id = %s",
                parameters=[ddl_script, schema_info.get('row_count', 0), job_id]
            )

            # Отправка уведомления
            self._send_progress_notification(job_id, 'analysis', {
                'status': 'completed',
                'schema_info': schema_info,
                'ddl_script': ddl_script
            })

            return {
                'job_id': job_id,
                'schema_info': schema_info,
                'ddl_script': ddl_script
            }

        except Exception as e:
            logger.error(f"File analysis failed for job {job_id}: {str(e)}")
            self._update_job_status(job_id, 'failed', f'Analysis failed: {str(e)}')
            raise

    def _create_destination_schema(self, job_config: Dict[str, Any], **context):
        """Создание схемы в целевой системе"""

        job_id = job_config['job_id']
        destination_type = job_config['destination_type']

        logger.info(f"Creating schema for job {job_id} in {destination_type}")

        try:
            # Получение DDL скрипта из предыдущей задачи
            ti = context['task_instance']
            analysis_result = ti.xcom_pull(task_ids='analyze_file_and_generate_ddl')
            ddl_script = analysis_result['ddl_script']

            # Выполнение DDL в зависимости от типа destination
            if destination_type == 'postgresql':
                self._execute_postgresql_ddl(job_config, ddl_script)
            elif destination_type == 'clickhouse':
                self._execute_clickhouse_ddl(job_config, ddl_script)
            elif destination_type == 'hdfs':
                self._prepare_hdfs_location(job_config)

            # Отправка уведомления
            self._send_progress_notification(job_id, 'schema_creation', {
                'status': 'completed',
                'destination_type': destination_type
            })

            return {
                'job_id': job_id,
                'schema_created': True,
                'destination_type': destination_type
            }

        except Exception as e:
            logger.error(f"Schema creation failed for job {job_id}: {str(e)}")
            self._update_job_status(job_id, 'failed', f'Schema creation failed: {str(e)}')
            raise

    def _generate_dlt_command(self, job_config: Dict[str, Any]) -> str:
        """Генерация команды для запуска dlt worker"""

        job_id = job_config['job_id']

        return f"""
        cd /app && python -c "
import os
import json
import sys
sys.path.append('/app')

from dlt_worker.main import DLTWorker
import asyncio

job_config = json.loads(os.environ['DLT_JOB_CONFIG'])
worker = DLTWorker()

async def run_job():
    try:
        result = await worker.process_job(job_config)
        print(f'DLT processing completed: {{result}}')
        sys.exit(0 if result.get('status') != 'failed' else 1)
    except Exception as e:
        print(f'DLT processing failed: {{e}}')
        sys.exit(1)

asyncio.run(run_job())
"
        """

    def _validate_results(self, job_config: Dict[str, Any], **context):
        """Валидация результатов обработки"""

        job_id = job_config['job_id']
        destination_type = job_config['destination_type']

        logger.info(f"Validating results for job {job_id}")

        try:
            # Проверка количества загруженных записей
            if destination_type == 'postgresql':
                record_count = self._count_postgresql_records(job_config)
            elif destination_type == 'clickhouse':
                record_count = self._count_clickhouse_records(job_config)
            elif destination_type == 'hdfs':
                record_count = self._count_hdfs_records(job_config)
            else:
                raise ValueError(f"Unsupported destination type: {destination_type}")

            # Обновление финального количества записей
            pg_hook = PostgresHook(postgres_conn_id='bigdata_postgres')
            pg_hook.run(
                "UPDATE processing_jobs SET records_processed = %s WHERE job_id = %s",
                parameters=[record_count, job_id]
            )

            # Отправка уведомления
            self._send_progress_notification(job_id, 'validation', {
                'status': 'completed',
                'records_loaded': record_count
            })

            return {
                'job_id': job_id,
                'records_loaded': record_count,
                'validation_status': 'success'
            }

        except Exception as e:
            logger.error(f"Results validation failed for job {job_id}: {str(e)}")
            self._update_job_status(job_id, 'failed', f'Validation failed: {str(e)}')
            raise

    def _cleanup_on_failure(self, job_config: Dict[str, Any], **context):
        """Очистка ресурсов при ошибке"""

        job_id = job_config['job_id']

        logger.info(f"Performing cleanup for failed job {job_id}")

        try:
            # Удаление временных файлов
            temp_files = [
                f"/app/data/temp/{job_id}_*",
                f"/app/data/processed/{job_id}_*"
            ]

            for pattern in temp_files:
                os.system(f"rm -f {pattern}")

            # Отправка уведомления об ошибке
            self._send_error_notification(job_id, context.get('reason', 'Unknown error'))

            return {'cleanup_completed': True}

        except Exception as e:
            logger.error(f"Cleanup failed for job {job_id}: {str(e)}")

    def _update_job_status(self, job_id: str, status: str, message: Optional[str] = None):
        """Обновление статуса задачи в БД"""

        try:
            pg_hook = PostgresHook(postgres_conn_id='bigdata_postgres')

            if message:
                pg_hook.run(
                    "UPDATE processing_jobs SET status = %s, error_message = %s WHERE job_id = %s",
                    parameters=[status, message, job_id]
                )
            else:
                pg_hook.run(
                    "UPDATE processing_jobs SET status = %s WHERE job_id = %s",
                    parameters=[status, job_id]
                )

        except Exception as e:
            logger.error(f"Failed to update job status: {e}")

    def _send_progress_notification(self, job_id: str, stage: str, details: Dict[str, Any]):
        """Отправка уведомления о прогрессе"""

        try:
            import requests

            notification_data = {
                'job_id': job_id,
                'type': 'stage_completed',
                'stage': stage,
                'details': details,
                'timestamp': datetime.utcnow().isoformat()
            }

            response = requests.post(
                'http://backend:8000/api/v1/notify',
                json=notification_data,
                timeout=10
            )
            response.raise_for_status()

        except Exception as e:
            logger.warning(f"Failed to send progress notification: {e}")

    def _send_error_notification(self, job_id: str, error_message: str):
        """Отправка уведомления об ошибке"""

        try:
            import requests

            notification_data = {
                'job_id': job_id,
                'type': 'job_error',
                'error': error_message,
                'timestamp': datetime.utcnow().isoformat()
            }

            response = requests.post(
                'http://backend:8000/api/v1/notify',
                json=notification_data,
                timeout=10
            )
            response.raise_for_status()

        except Exception as e:
            logger.warning(f"Failed to send error notification: {e}")

    # Destination-specific methods
    def _execute_postgresql_ddl(self, job_config: Dict[str, Any], ddl_script: str):
        """Выполнение DDL скрипта в PostgreSQL"""

        pg_hook = PostgresHook(postgres_conn_id='user_data_postgres')
        pg_hook.run(ddl_script)

    def _execute_clickhouse_ddl(self, job_config: Dict[str, Any], ddl_script: str):
        """Выполнение DDL скрипта в ClickHouse"""

        from airflow.providers.http.hooks.http import HttpHook

        http_hook = HttpHook(method='POST', http_conn_id='clickhouse_http')
        response = http_hook.run(
            endpoint='/',
            data=ddl_script,
            headers={'Content-Type': 'text/plain'}
        )

    def _prepare_hdfs_location(self, job_config: Dict[str, Any]):
        """Подготовка директории в HDFS"""

        from airflow.operators.bash import BashOperator

        hdfs_path = job_config['destination_config']['base_path']
        job_id = job_config['job_id']

        commands = [
            f"hdfs dfs -mkdir -p {hdfs_path}/{job_id}",
            f"hdfs dfs -chmod 755 {hdfs_path}/{job_id}"
        ]

        for cmd in commands:
            os.system(cmd)

    def _count_postgresql_records(self, job_config: Dict[str, Any]) -> int:
        """Подсчет записей в PostgreSQL"""

        pg_hook = PostgresHook(postgres_conn_id='user_data_postgres')

        table_name = self._get_table_name(job_config)
        result = pg_hook.get_first(f"SELECT COUNT(*) FROM {table_name}")

        return result[0] if result else 0

    def _count_clickhouse_records(self, job_config: Dict[str, Any]) -> int:
        """Подсчет записей в ClickHouse"""

        from airflow.providers.http.hooks.http import HttpHook

        http_hook = HttpHook(method='POST', http_conn_id='clickhouse_http')
        table_name = self._get_table_name(job_config)

        response = http_hook.run(
            endpoint='/',
            data=f"SELECT COUNT(*) FROM {table_name} FORMAT JSON",
            headers={'Content-Type': 'text/plain'}
        )

        result = json.loads(response.text)
        return result['data'][0]['count()']

    def _count_hdfs_records(self, job_config: Dict[str, Any]) -> int:
        """Подсчет записей в HDFS (через метаданные)"""

        # Для HDFS используем информацию из файлов или метаданных dlt
        job_id = job_config['job_id']

        # Предполагаем, что dlt сохраняет метаданные о количестве записей
        metadata_file = f"/app/data/processed/{job_id}_metadata.json"

        if os.path.exists(metadata_file):
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
                return metadata.get('record_count', 0)

        return 0

    def _get_table_name(self, job_config: Dict[str, Any]) -> str:
        """Получение имени таблицы для задачи"""

        destination_config = job_config['destination_config']
        table_prefix = destination_config.get('table_prefix', 'imported_')
        job_id = job_config['job_id']

        return f"{table_prefix}{job_id}"


# Глобальный экземпляр фабрики
dag_factory = ETLDAGFactory()
```

## API для управления DAG'ами

### dags/api/dag_management.py
```python
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from airflow.models import DagBag, DagRun
from airflow.api.client.local_client import Client
from airflow.configuration import conf
import os

logger = logging.getLogger(__name__)

class AirflowDAGManager:
    """Менеджер для управления динамическими DAG'ами"""

    def __init__(self):
        self.client = Client(None, None)
        self.dag_folder = conf.get('core', 'DAGS_FOLDER')

    def create_dag_file(self, job_config: Dict[str, Any]) -> str:
        """Создание файла DAG для задачи"""

        job_id = job_config['job_id']
        dag_filename = f"etl_dag_{job_id}.py"
        dag_filepath = os.path.join(self.dag_folder, dag_filename)

        dag_content = self._generate_dag_content(job_config)

        try:
            with open(dag_filepath, 'w') as f:
                f.write(dag_content)

            logger.info(f"Created DAG file: {dag_filepath}")
            return dag_filepath

        except Exception as e:
            logger.error(f"Failed to create DAG file: {e}")
            raise

    def _generate_dag_content(self, job_config: Dict[str, Any]) -> str:
        """Генерация содержимого DAG файла"""

        job_id = job_config['job_id']

        dag_content = f'''
# Auto-generated DAG for job {job_id}
# Generated at: {datetime.utcnow().isoformat()}

import json
from dags.dynamic_etl_dag_factory import dag_factory

# Job configuration
job_config = {json.dumps(job_config, indent=2)}

# Create DAG
dag = dag_factory.create_etl_dag(job_config)

# Export DAG
globals()[f"etl_job_{job_id}"] = dag
'''

        return dag_content

    def trigger_dag(self, job_id: str, job_config: Dict[str, Any]) -> str:
        """Запуск DAG для задачи"""

        dag_id = f"etl_job_{job_id}"

        try:
            # Создание файла DAG
            dag_filepath = self.create_dag_file(job_config)

            # Ожидание загрузки DAG в Airflow (может потребоваться время)
            import time
            time.sleep(5)  # Базовая задержка для загрузки DAG

            # Запуск DAG
            dag_run = self.client.trigger_dag(
                dag_id=dag_id,
                run_id=f"manual__{datetime.utcnow().isoformat()}",
                conf=job_config
            )

            logger.info(f"Triggered DAG {dag_id} with run_id: {dag_run.run_id}")
            return dag_run.run_id

        except Exception as e:
            logger.error(f"Failed to trigger DAG {dag_id}: {e}")
            raise

    def get_dag_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Получение статуса DAG"""

        dag_id = f"etl_job_{job_id}"

        try:
            # Получение последнего запуска DAG
            dag_runs = self.client.get_dag_runs(dag_id=dag_id, limit=1)

            if not dag_runs:
                return None

            dag_run = dag_runs[0]

            # Получение статусов задач
            task_instances = self.client.get_task_instances(
                dag_id=dag_id,
                dag_run_id=dag_run.run_id
            )

            task_statuses = {}
            for ti in task_instances:
                task_statuses[ti.task_id] = {
                    'state': ti.state,
                    'start_date': ti.start_date.isoformat() if ti.start_date else None,
                    'end_date': ti.end_date.isoformat() if ti.end_date else None,
                    'duration': ti.duration
                }

            return {
                'dag_id': dag_id,
                'run_id': dag_run.run_id,
                'state': dag_run.state,
                'start_date': dag_run.start_date.isoformat() if dag_run.start_date else None,
                'end_date': dag_run.end_date.isoformat() if dag_run.end_date else None,
                'tasks': task_statuses
            }

        except Exception as e:
            logger.error(f"Failed to get DAG status for {dag_id}: {e}")
            return None

    def cancel_dag_run(self, job_id: str) -> bool:
        """Отмена выполнения DAG"""

        dag_id = f"etl_job_{job_id}"

        try:
            # Получение активных запусков DAG
            active_runs = self.client.get_dag_runs(
                dag_id=dag_id,
                state=['running', 'queued']
            )

            for dag_run in active_runs:
                # Остановка задач
                task_instances = self.client.get_task_instances(
                    dag_id=dag_id,
                    dag_run_id=dag_run.run_id,
                    state=['running', 'queued']
                )

                for ti in task_instances:
                    self.client.clear_task_instances(
                        dag_id=dag_id,
                        task_ids=[ti.task_id],
                        start_date=dag_run.start_date,
                        end_date=dag_run.end_date,
                        include_subdags=True
                    )

            logger.info(f"Cancelled DAG runs for {dag_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to cancel DAG {dag_id}: {e}")
            return False

    def cleanup_dag(self, job_id: str) -> bool:
        """Очистка DAG и связанных файлов"""

        dag_filename = f"etl_dag_{job_id}.py"
        dag_filepath = os.path.join(self.dag_folder, dag_filename)

        try:
            # Удаление файла DAG
            if os.path.exists(dag_filepath):
                os.remove(dag_filepath)
                logger.info(f"Removed DAG file: {dag_filepath}")

            # Очистка метаданных DAG из Airflow (опционально)
            # Это может потребовать дополнительных прав доступа

            return True

        except Exception as e:
            logger.error(f"Failed to cleanup DAG {job_id}: {e}")
            return False

# Глобальный экземпляр менеджера
dag_manager = AirflowDAGManager()
```

## Интеграция с Backend

### Обновление core/airflow_client.py
```python
import aiohttp
import asyncio
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class AirflowClient:
    """Клиент для взаимодействия с Airflow API"""

    def __init__(self):
        self.base_url = "http://airflow-webserver:8080/api/v1"
        self.auth = ('admin', 'admin')  # В production использовать proper auth

    async def trigger_dag(self, job_id: str, job_config: Dict[str, Any]) -> str:
        """Запуск DAG через API"""

        # Сначала создаем DAG файл через специальный endpoint
        await self._create_dag_file(job_id, job_config)

        # Ждем загрузки DAG
        await asyncio.sleep(5)

        # Запускаем DAG
        dag_id = f"etl_job_{job_id}"
        run_id = f"manual__{datetime.utcnow().isoformat()}"

        async with aiohttp.ClientSession() as session:
            url = f"{self.base_url}/dags/{dag_id}/dagRuns"

            payload = {
                "dag_run_id": run_id,
                "conf": job_config
            }

            async with session.post(
                url,
                json=payload,
                auth=aiohttp.BasicAuth(*self.auth),
                headers={'Content-Type': 'application/json'}
            ) as response:

                if response.status == 200:
                    result = await response.json()
                    logger.info(f"DAG {dag_id} triggered successfully")
                    return run_id
                else:
                    error_text = await response.text()
                    raise Exception(f"Failed to trigger DAG: {error_text}")

    async def _create_dag_file(self, job_id: str, job_config: Dict[str, Any]):
        """Создание DAG файла через кастомный endpoint"""

        async with aiohttp.ClientSession() as session:
            url = f"{self.base_url}/custom/create_dag"

            payload = {
                'job_id': job_id,
                'job_config': job_config
            }

            async with session.post(
                url,
                json=payload,
                auth=aiohttp.BasicAuth(*self.auth),
                headers={'Content-Type': 'application/json'}
            ) as response:

                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"Failed to create DAG file: {error_text}")

    async def get_dag_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Получение статуса DAG"""

        dag_id = f"etl_job_{job_id}"

        async with aiohttp.ClientSession() as session:
            # Получение последнего запуска DAG
            url = f"{self.base_url}/dags/{dag_id}/dagRuns"

            async with session.get(
                url,
                auth=aiohttp.BasicAuth(*self.auth),
                params={'limit': 1, 'order_by': '-start_date'}
            ) as response:

                if response.status == 200:
                    result = await response.json()
                    dag_runs = result.get('dag_runs', [])

                    if not dag_runs:
                        return None

                    dag_run = dag_runs[0]

                    # Получение статусов задач
                    task_statuses = await self._get_task_statuses(dag_id, dag_run['dag_run_id'])

                    return {
                        'dag_id': dag_id,
                        'run_id': dag_run['dag_run_id'],
                        'state': dag_run['state'],
                        'start_date': dag_run['start_date'],
                        'end_date': dag_run['end_date'],
                        'tasks': task_statuses
                    }
                else:
                    logger.error(f"Failed to get DAG status: {response.status}")
                    return None

    async def _get_task_statuses(self, dag_id: str, run_id: str) -> Dict[str, Any]:
        """Получение статусов задач DAG"""

        async with aiohttp.ClientSession() as session:
            url = f"{self.base_url}/dags/{dag_id}/dagRuns/{run_id}/taskInstances"

            async with session.get(
                url,
                auth=aiohttp.BasicAuth(*self.auth)
            ) as response:

                if response.status == 200:
                    result = await response.json()
                    task_instances = result.get('task_instances', [])

                    task_statuses = {}
                    for ti in task_instances:
                        task_statuses[ti['task_id']] = {
                            'state': ti['state'],
                            'start_date': ti['start_date'],
                            'end_date': ti['end_date'],
                            'duration': ti['duration']
                        }

                    return task_statuses
                else:
                    logger.error(f"Failed to get task statuses: {response.status}")
                    return {}

    async def cancel_dag_run(self, job_id: str) -> bool:
        """Отмена выполнения DAG"""

        dag_id = f"etl_job_{job_id}"

        try:
            # Получение активных запусков
            status = await self.get_dag_status(job_id)
            if not status or status['state'] not in ['running', 'queued']:
                return True

            run_id = status['run_id']

            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/dags/{dag_id}/dagRuns/{run_id}"

                payload = {"state": "failed"}

                async with session.patch(
                    url,
                    json=payload,
                    auth=aiohttp.BasicAuth(*self.auth),
                    headers={'Content-Type': 'application/json'}
                ) as response:

                    if response.status == 200:
                        logger.info(f"DAG run {run_id} cancelled")
                        return True
                    else:
                        logger.error(f"Failed to cancel DAG run: {response.status}")
                        return False

        except Exception as e:
            logger.error(f"Error cancelling DAG: {e}")
            return False
```

## Файл утилит для анализа файлов

### dags/utils/file_analyzer.py
```python
import pandas as pd
import json
import xml.etree.ElementTree as ET
import logging
from typing import Dict, Any, List
import os

logger = logging.getLogger(__name__)

class FileAnalyzer:
    """Анализатор структуры файлов"""

    def analyze_file(self, file_path: str, file_format: str) -> Dict[str, Any]:
        """Анализ структуры файла"""

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        if file_format == 'csv':
            return self._analyze_csv(file_path)
        elif file_format == 'json':
            return self._analyze_json(file_path)
        elif file_format == 'xml':
            return self._analyze_xml(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

    def _analyze_csv(self, file_path: str) -> Dict[str, Any]:
        """Анализ CSV файла"""

        try:
            # Чтение первых строк для анализа
            sample_df = pd.read_csv(file_path, nrows=1000)

            # Получение информации о колонках
            columns_info = []
            for col in sample_df.columns:
                col_info = {
                    'name': str(col).strip(),
                    'dtype': str(sample_df[col].dtype),
                    'nullable': sample_df[col].isnull().any(),
                    'sample_values': sample_df[col].dropna().head(3).tolist()
                }
                columns_info.append(col_info)

            # Подсчет общего количества строк
            row_count = sum(1 for _ in open(file_path)) - 1  # -1 для заголовка

            return {
                'file_format': 'csv',
                'columns': columns_info,
                'row_count': row_count,
                'sample_data': sample_df.head(5).to_dict('records')
            }

        except Exception as e:
            logger.error(f"CSV analysis failed: {e}")
            raise

    def _analyze_json(self, file_path: str) -> Dict[str, Any]:
        """Анализ JSON файла"""

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # Определяем тип JSON (массив или JSONL)
                first_line = f.readline().strip()
                f.seek(0)

                if first_line.startswith('['):
                    # JSON массив
                    data = json.load(f)
                    if isinstance(data, list) and len(data) > 0:
                        sample_record = data[0]
                        row_count = len(data)
                    else:
                        raise ValueError("Empty JSON array")
                else:
                    # JSONL формат
                    records = []
                    row_count = 0
                    for line in f:
                        if line.strip():
                            records.append(json.loads(line))
                            row_count += 1
                            if len(records) >= 1000:  # Лимит для анализа
                                break

                    if records:
                        sample_record = records[0]
                    else:
                        raise ValueError("No valid JSON records found")

            # Анализ структуры
            columns_info = self._analyze_json_structure(sample_record)

            return {
                'file_format': 'json',
                'columns': columns_info,
                'row_count': row_count,
                'sample_data': [sample_record] if isinstance(sample_record, dict) else sample_record[:5]
            }

        except Exception as e:
            logger.error(f"JSON analysis failed: {e}")
            raise

    def _analyze_xml(self, file_path: str) -> Dict[str, Any]:
        """Анализ XML файла"""

        try:
            tree = ET.parse(file_path)
            root = tree.getroot()

            # Поиск повторяющихся элементов (записей)
            record_elements = self._find_record_elements(root)

            if not record_elements:
                raise ValueError("No repeating record elements found in XML")

            # Анализ структуры первой записи
            sample_record = record_elements[0]
            columns_info = self._analyze_xml_structure(sample_record)

            return {
                'file_format': 'xml',
                'columns': columns_info,
                'row_count': len(record_elements),
                'sample_data': [self._xml_to_dict(elem) for elem in record_elements[:5]]
            }

        except Exception as e:
            logger.error(f"XML analysis failed: {e}")
            raise

    def _analyze_json_structure(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Анализ структуры JSON записи"""

        columns_info = []

        for key, value in record.items():
            col_info = {
                'name': key,
                'dtype': self._get_python_type(value),
                'nullable': value is None,
                'sample_values': [value] if value is not None else []
            }
            columns_info.append(col_info)

        return columns_info

    def _analyze_xml_structure(self, element: ET.Element) -> List[Dict[str, Any]]:
        """Анализ структуры XML элемента"""

        columns_info = []

        # Атрибуты элемента
        for attr_name, attr_value in element.attrib.items():
            col_info = {
                'name': f"@{attr_name}",
                'dtype': self._get_python_type(attr_value),
                'nullable': False,
                'sample_values': [attr_value]
            }
            columns_info.append(col_info)

        # Дочерние элементы
        for child in element:
            col_info = {
                'name': child.tag,
                'dtype': self._get_python_type(child.text),
                'nullable': child.text is None or child.text.strip() == '',
                'sample_values': [child.text] if child.text else []
            }
            columns_info.append(col_info)

        return columns_info

    def _find_record_elements(self, root: ET.Element) -> List[ET.Element]:
        """Поиск повторяющихся элементов в XML"""

        # Подсчет частоты тегов на каждом уровне
        tag_counts = {}

        def count_tags(element, level=0):
            if level not in tag_counts:
                tag_counts[level] = {}

            tag = element.tag
            if tag not in tag_counts[level]:
                tag_counts[level][tag] = 0
            tag_counts[level][tag] += 1

            for child in element:
                count_tags(child, level + 1)

        count_tags(root)

        # Поиск наиболее часто встречающегося тега (предполагаемые записи)
        max_count = 0
        record_tag = None

        for level, tags in tag_counts.items():
            for tag, count in tags.items():
                if count > max_count and count > 1:  # Более одного элемента
                    max_count = count
                    record_tag = tag

        if record_tag:
            return root.findall(f".//{record_tag}")

        return []

    def _xml_to_dict(self, element: ET.Element) -> Dict[str, Any]:
        """Конвертация XML элемента в словарь"""

        result = {}

        # Атрибуты
        for attr_name, attr_value in element.attrib.items():
            result[f"@{attr_name}"] = attr_value

        # Дочерние элементы
        for child in element:
            result[child.tag] = child.text

        return result

    def _get_python_type(self, value: Any) -> str:
        """Определение типа Python для значения"""

        if value is None:
            return 'string'

        if isinstance(value, bool):
            return 'boolean'
        elif isinstance(value, int):
            return 'integer'
        elif isinstance(value, float):
            return 'float'
        elif isinstance(value, str):
            # Попытка определить более специфичный тип
            if self._is_date_string(value):
                return 'datetime'
            elif self._is_number_string(value):
                return 'float' if '.' in value else 'integer'
            else:
                return 'string'
        else:
            return 'string'

    def _is_date_string(self, value: str) -> bool:
        """Проверка, является ли строка датой"""

        import dateutil.parser

        try:
            dateutil.parser.parse(value)
            return True
        except:
            return False

    def _is_number_string(self, value: str) -> bool:
        """Проверка, является ли строка числом"""

        try:
            float(value)
            return True
        except:
            return False
```

## Задачи для выполнения

### 1. Доработка Airflow DAG Factory
- [ ] Реализовать ETLDAGFactory с методами для всех этапов
- [ ] Добавить callback систему для уведомлений
- [ ] Интеграция с dlt worker через Bash команды
- [ ] Error handling и retry логика

### 2. Создание DAG Management API
- [ ] AirflowDAGManager для управления динамическими DAG'ами
- [ ] Методы создания, запуска и остановки DAG'ов
- [ ] Очистка временных файлов и метаданных
- [ ] Статусы выполнения задач

### 3. Интеграция с Backend
- [ ] Обновление AirflowClient в backend
- [ ] Асинхронные методы для работы с Airflow API
- [ ] Обработка ошибок и timeout'ов
- [ ] Мониторинг статуса выполнения

### 4. File Analysis утилиты
- [ ] FileAnalyzer для анализа CSV, JSON, XML
- [ ] Определение схемы и типов данных
- [ ] Генерация примеров данных
- [ ] Подсчет количества записей

### 5. DDL Generator
- [ ] Генерация DDL скриптов для PostgreSQL
- [ ] Генерация DDL скриптов для ClickHouse
- [ ] Подготовка структур для HDFS
- [ ] Адаптация под различные типы данных

### 6. Мониторинг и уведомления
- [ ] Система callback'ов для каждого этапа
- [ ] WebSocket уведомления о прогрессе
- [ ] Логирование всех операций
- [ ] Метрики производительности

### 7. Error Handling и Recovery
- [ ] Retry логика для failed задач
- [ ] Cleanup при ошибках
- [ ] Уведомления об ошибках
- [ ] Восстановление после сбоев

## Ожидаемый результат
После выполнения этой задачи должна быть готова полная интеграция Airflow с dlt worker, обеспечивающая надежное выполнение ETL процессов с мониторингом и error handling.

## Следующая задача
[11_testing_big_files.md](./11_testing_big_files.md) - Тестирование с большими файлами