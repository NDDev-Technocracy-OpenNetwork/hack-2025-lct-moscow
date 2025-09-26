from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import json
import logging
import subprocess
import os

from utils.dlt_operators import DltETLOperator
from utils.notification_operators import WebSocketNotifyOperator
from utils.file_validators import FileValidatorOperator

logger = logging.getLogger(__name__)


def create_dynamic_etl_dag(job_config: dict):
    """
    Create dynamic DAG for processing uploaded files

    Args:
        job_config: dict with job configuration
            - job_id: UUID of the job
            - filename: name of the file
            - file_format: file format (csv, json, xml)
            - destination_type: destination type (postgresql, clickhouse, hdfs)
            - destination_config: destination configuration
    """

    dag_id = f"etl_job_{job_config['job_id']}"

    default_args = {
        'owner': 'bigdata-system',
        'depends_on_past': False,
        'start_date': datetime.now(),
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': notify_failure,
        'on_success_callback': notify_success,
        'execution_timeout': timedelta(hours=2),  # 2 hour timeout for large files
    }

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f'ETL process for {job_config.get("filename", "unknown")} ({job_config.get("file_format", "unknown")})',
        schedule_interval=None,  # Only triggered on demand
        max_active_runs=1,
        catchup=False,
        tags=['etl', 'dynamic', job_config.get('file_format', 'unknown')],
        doc_md=f"""
        ## Dynamic ETL DAG

        **Job ID:** {job_config.get('job_id', 'unknown')}
        **File:** {job_config.get('filename', 'unknown')}
        **Format:** {job_config.get('file_format', 'unknown')}
        **Destination:** {job_config.get('destination_type', 'unknown')}

        This DAG processes uploaded files through the following steps:
        1. Validate file integrity and format
        2. Analyze file structure and generate DDL
        3. Process data using DLT worker
        4. Send progress notifications
        5. Finalize job and update status
        """,
    )

    # Task 1: Validate file
    validate_file_task = FileValidatorOperator(
        task_id='validate_file',
        job_config=job_config,
        dag=dag
    )

    # Task 2: Analyze file structure
    @task(dag=dag)
    def analyze_file_structure(**context):
        """Analyze file structure and generate DDL"""

        job_config = context['dag_run'].conf
        file_path = f"/app/data/input/{job_config['filename']}"

        try:
            # Send notification about analysis start
            notify_backend(job_config['job_id'], {
                'status': 'analyzing',
                'stage': 'file_analysis',
                'message': 'Starting file structure analysis'
            })

            # Import file processor from backend
            import sys
            sys.path.append('/app')

            from backend.core.file_processor import FileProcessor
            from backend.core.ddl_generator import DDLGenerator

            # Analyze file
            processor = FileProcessor()
            schema_info = processor.analyze_file(file_path, job_config['file_format'])

            # Generate DDL scripts
            ddl_generator = DDLGenerator()
            ddl_script = ddl_generator.generate_ddl(
                schema_info,
                job_config['destination_type'],
                job_config['destination_config']
            )
            etl_script = ddl_generator.generate_etl_script(
                schema_info,
                job_config['destination_type']
            )

            # Update job in database
            pg_hook = PostgresHook(postgres_conn_id='bigdata_postgres')
            pg_hook.run(
                """UPDATE processing_jobs
                   SET ddl_script = %s, etl_script = %s,
                       records_total = %s, status = 'analyzed'
                   WHERE job_id = %s""",
                parameters=[ddl_script, etl_script, schema_info.get('total_rows', 0), job_config['job_id']]
            )

            # Send analysis complete notification
            notify_backend(job_config['job_id'], {
                'status': 'analyzed',
                'stage': 'analysis_complete',
                'message': f'File analysis complete. Found {schema_info.get("total_rows", 0)} records',
                'total_records': schema_info.get('total_rows', 0)
            })

            logger.info(f"File analysis completed for job {job_config['job_id']}")
            return schema_info

        except Exception as e:
            logger.error(f"File analysis failed: {str(e)}")

            # Update job status to failed
            pg_hook = PostgresHook(postgres_conn_id='bigdata_postgres')
            pg_hook.run(
                "UPDATE processing_jobs SET status = 'failed', error_message = %s WHERE job_id = %s",
                parameters=[str(e), job_config['job_id']]
            )

            raise

    analyze_structure = analyze_file_structure()

    # Task 3: Process data with DLT worker
    process_data_task = DltETLOperator(
        task_id='process_with_dlt_worker',
        job_config=job_config,
        dag=dag
    )

    # Task 4: Send progress notification
    notify_progress_task = WebSocketNotifyOperator(
        task_id='notify_processing_complete',
        job_config=job_config,
        message_type='processing_complete',
        dag=dag
    )

    # Task 5: Finalize job
    @task(dag=dag)
    def finalize_job(**context):
        """Finalize job and update final status"""

        job_config = context['dag_run'].conf

        try:
            # Get final statistics from database
            pg_hook = PostgresHook(postgres_conn_id='bigdata_postgres')
            result = pg_hook.get_first(
                "SELECT records_processed, records_total FROM processing_jobs WHERE job_id = %s",
                parameters=[job_config['job_id']]
            )

            records_processed = result[0] if result else 0
            records_total = result[1] if result else 0

            # Update final status
            pg_hook.run(
                """UPDATE processing_jobs
                   SET status = 'completed',
                       completed_at = CURRENT_TIMESTAMP,
                       updated_at = CURRENT_TIMESTAMP
                   WHERE job_id = %s""",
                parameters=[job_config['job_id']]
            )

            # Send final notification
            notify_backend(job_config['job_id'], {
                'status': 'completed',
                'stage': 'finished',
                'message': f'ETL process completed successfully. Processed {records_processed}/{records_total} records',
                'records_processed': records_processed,
                'total_records': records_total,
                'progress_percent': 100.0
            })

            logger.info(f"Job {job_config['job_id']} completed successfully. Processed {records_processed} records")

            return {
                'status': 'completed',
                'records_processed': records_processed,
                'records_total': records_total
            }

        except Exception as e:
            logger.error(f"Job finalization failed: {str(e)}")

            # Update to failed status
            pg_hook = PostgresHook(postgres_conn_id='bigdata_postgres')
            pg_hook.run(
                "UPDATE processing_jobs SET status = 'failed', error_message = %s WHERE job_id = %s",
                parameters=[str(e), job_config['job_id']]
            )

            raise

    finalize_task = finalize_job()

    # Define task dependencies
    validate_file_task >> analyze_structure >> process_data_task >> notify_progress_task >> finalize_task

    return dag


def notify_failure(context):
    """Callback for failure notification"""
    try:
        job_config = context['dag_run'].conf
        job_id = job_config.get('job_id')
        task_instance = context.get('task_instance')
        exception = context.get('exception')

        error_msg = str(exception) if exception else f"Task {task_instance.task_id} failed"

        logger.error(f"Job {job_id} failed: {error_msg}")

        # Update database status
        pg_hook = PostgresHook(postgres_conn_id='bigdata_postgres')
        pg_hook.run(
            """UPDATE processing_jobs
               SET status = 'failed',
                   error_message = %s,
                   updated_at = CURRENT_TIMESTAMP
               WHERE job_id = %s""",
            parameters=[error_msg, job_id]
        )

        # Send failure notification
        notify_backend(job_id, {
            'status': 'failed',
            'stage': 'error',
            'error_message': error_msg,
            'message': f'Job failed at task {task_instance.task_id}: {error_msg}',
            'failed_task': task_instance.task_id
        })

    except Exception as e:
        logger.error(f"Error in failure callback: {str(e)}")


def notify_success(context):
    """Callback for success notification"""
    try:
        job_config = context['dag_run'].conf
        job_id = job_config.get('job_id')
        task_instance = context.get('task_instance')

        logger.info(f"Task {task_instance.task_id} completed successfully for job {job_id}")

        # Send task completion notification
        notify_backend(job_id, {
            'status': 'processing',
            'stage': f'{task_instance.task_id}_complete',
            'message': f'Task {task_instance.task_id} completed successfully',
            'completed_task': task_instance.task_id
        })

    except Exception as e:
        logger.error(f"Error in success callback: {str(e)}")


def notify_backend(job_id: str, notification_data: dict):
    """Send notification to backend via HTTP"""
    try:
        import requests

        url = f"http://backend:8000/api/v1/notify/{job_id}"

        payload = {
            'type': 'airflow_notification',
            'timestamp': datetime.utcnow().isoformat(),
            **notification_data
        }

        response = requests.post(url, json=payload, timeout=10)

        if response.status_code == 200:
            logger.debug(f"Notification sent successfully for job {job_id}")
        else:
            logger.warning(f"Failed to send notification: HTTP {response.status_code}")

    except Exception as e:
        logger.warning(f"Error sending notification to backend: {str(e)}")


# Factory function to create DAGs dynamically
def create_dag_from_config(dag_id: str, job_config: dict):
    """Factory function for creating DAGs from configuration"""
    return create_dynamic_etl_dag(job_config)


# This section registers DAGs that are created dynamically
# The actual DAG registration happens through the Airflow API
# when jobs are submitted via the backend

# For development/testing purposes, we can create a sample DAG
if os.getenv('AIRFLOW_ENV') == 'development':
    sample_config = {
        'job_id': 'sample-job-123',
        'filename': 'sample.csv',
        'file_format': 'csv',
        'destination_type': 'postgresql',
        'destination_config': {
            'host': 'postgres',
            'port': 5432,
            'database': 'bigdata_db',
            'schema_name': 'public',
            'table_name': 'sample_data'
        }
    }

    sample_dag = create_dynamic_etl_dag(sample_config)
    globals()[sample_dag.dag_id] = sample_dag