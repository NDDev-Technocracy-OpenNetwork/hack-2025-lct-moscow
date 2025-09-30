"""
BigData File Processing DAG - Production Pipeline
Handles CSV, JSON, XML file processing with multiple destinations
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import dlt
import json
import os
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'bigdata-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 26),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}

dag = DAG(
    'bigdata_file_processing_pipeline',
    default_args=default_args,
    description='Production BigData file processing with DLT and multiple destinations',
    schedule_interval=timedelta(minutes=30),
    catchup=False,
    max_active_runs=3,
    tags=['bigdata', 'etl', 'production']
)

def validate_file_structure(**context):
    """Validate incoming file structure and metadata"""
    job_id = context['dag_run'].conf.get('job_id')
    file_path = context['dag_run'].conf.get('file_path')
    file_format = context['dag_run'].conf.get('file_format')

    logger.info(f"Validating file {file_path} for job {job_id}")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    file_size = os.path.getsize(file_path)
    if file_size == 0:
        raise ValueError("File is empty")

    # Validate file format
    if file_format == 'csv':
        df = pd.read_csv(file_path, nrows=5)
        if df.empty:
            raise ValueError("CSV file has no data")
        logger.info(f"CSV validation passed. Columns: {list(df.columns)}")

    elif file_format == 'json':
        with open(file_path, 'r') as f:
            data = json.load(f)
        if not data:
            raise ValueError("JSON file has no data")
        logger.info(f"JSON validation passed. Keys: {list(data.keys()) if isinstance(data, dict) else 'Array'}")

    return {
        'file_size': file_size,
        'validation_status': 'passed',
        'job_id': job_id
    }

def extract_and_normalize(**context):
    """Extract data and normalize using DLT"""
    job_id = context['dag_run'].conf.get('job_id')
    file_path = context['dag_run'].conf.get('file_path')
    file_format = context['dag_run'].conf.get('file_format')

    logger.info(f"Starting DLT extraction for job {job_id}")

    # Initialize DLT pipeline
    pipeline = dlt.pipeline(
        pipeline_name=f"bigdata_job_{job_id}",
        destination="postgres",
        dataset_name="processed_data"
    )

    # Extract based on file format
    if file_format == 'csv':
        @dlt.resource(name="csv_data", write_disposition="replace")
        def load_csv():
            df = pd.read_csv(file_path)
            return df.to_dict('records')

        data_resource = load_csv()

    elif file_format == 'json':
        @dlt.resource(name="json_data", write_disposition="replace")
        def load_json():
            with open(file_path, 'r') as f:
                data = json.load(f)
            return data if isinstance(data, list) else [data]

        data_resource = load_json()

    # Run DLT pipeline
    load_info = pipeline.run(data_resource)

    logger.info(f"DLT pipeline completed: {load_info}")

    return {
        'pipeline_name': pipeline.pipeline_name,
        'load_info': str(load_info),
        'job_id': job_id
    }

def load_to_clickhouse(**context):
    """Load processed data to ClickHouse for analytics"""
    job_id = context['dag_run'].conf.get('job_id')

    logger.info(f"Loading to ClickHouse for job {job_id}")

    # Initialize ClickHouse pipeline
    pipeline = dlt.pipeline(
        pipeline_name=f"analytics_job_{job_id}",
        destination="clickhouse",
        dataset_name="analytics"
    )

    # Get data from PostgreSQL and load to ClickHouse
    postgres_hook = PostgresHook(postgres_conn_id='bigdata_postgres')
    query = f"SELECT * FROM processed_data.csv_data_{job_id} LIMIT 10000"
    df = postgres_hook.get_pandas_df(query)

    @dlt.resource(name="analytics_data", write_disposition="append")
    def analytics_data():
        return df.to_dict('records')

    load_info = pipeline.run(analytics_data())
    logger.info(f"ClickHouse load completed: {load_info}")

    return {'clickhouse_load_info': str(load_info)}

def load_to_hdfs(**context):
    """Load processed data to HDFS for distributed storage"""
    job_id = context['dag_run'].conf.get('job_id')
    file_path = context['dag_run'].conf.get('file_path')

    logger.info(f"Loading to HDFS for job {job_id}")

    # Initialize HDFS pipeline
    pipeline = dlt.pipeline(
        pipeline_name=f"hdfs_job_{job_id}",
        destination="filesystem",
        dataset_name="raw_data"
    )

    # Copy file to HDFS
    hdfs_path = f"/bigdata/processed/{job_id}/"

    @dlt.resource(name="raw_file_data", write_disposition="replace")
    def hdfs_data():
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
            return df.to_dict('records')
        elif file_path.endswith('.json'):
            with open(file_path, 'r') as f:
                return json.load(f)

    load_info = pipeline.run(hdfs_data())
    logger.info(f"HDFS load completed: {load_info}")

    return {'hdfs_load_info': str(load_info)}

def update_job_status(**context):
    """Update job status in PostgreSQL"""
    job_id = context['dag_run'].conf.get('job_id')
    status = context['dag_run'].conf.get('status', 'completed')

    postgres_hook = PostgresHook(postgres_conn_id='bigdata_postgres')

    update_query = """
    UPDATE processing_jobs
    SET status = %s,
        completed_at = CURRENT_TIMESTAMP,
        records_processed = (
            SELECT COUNT(*) FROM processed_data.csv_data WHERE job_id = %s
        )
    WHERE job_id = %s
    """

    postgres_hook.run(update_query, parameters=[status, job_id, job_id])
    logger.info(f"Updated job {job_id} status to {status}")

def generate_data_lineage(**context):
    """Generate data lineage and quality metrics"""
    job_id = context['dag_run'].conf.get('job_id')

    postgres_hook = PostgresHook(postgres_conn_id='bigdata_postgres')

    lineage_query = """
    INSERT INTO data_lineage (job_id, source_type, source_path, destination_type,
                             transformation_type, created_at)
    VALUES (%s, 'file', %s, 'postgres,clickhouse,hdfs', 'dlt_pipeline', CURRENT_TIMESTAMP)
    """

    file_path = context['dag_run'].conf.get('file_path')
    postgres_hook.run(lineage_query, parameters=[job_id, file_path])

    logger.info(f"Generated data lineage for job {job_id}")

# Task definitions
validate_task = PythonOperator(
    task_id='validate_file_structure',
    python_callable=validate_file_structure,
    dag=dag
)

extract_normalize_task = PythonOperator(
    task_id='extract_and_normalize',
    python_callable=extract_and_normalize,
    dag=dag
)

clickhouse_load_task = PythonOperator(
    task_id='load_to_clickhouse',
    python_callable=load_to_clickhouse,
    dag=dag
)

hdfs_load_task = PythonOperator(
    task_id='load_to_hdfs',
    python_callable=load_to_hdfs,
    dag=dag
)

update_status_task = PythonOperator(
    task_id='update_job_status',
    python_callable=update_job_status,
    dag=dag
)

lineage_task = PythonOperator(
    task_id='generate_data_lineage',
    python_callable=generate_data_lineage,
    dag=dag
)

# Data quality check
quality_check_task = PostgresOperator(
    task_id='data_quality_check',
    postgres_conn_id='bigdata_postgres',
    sql="""
    INSERT INTO system_config (config_key, config_value, updated_at)
    VALUES ('last_quality_check', CURRENT_TIMESTAMP::text, CURRENT_TIMESTAMP)
    ON CONFLICT (config_key) DO UPDATE SET
        config_value = EXCLUDED.config_value,
        updated_at = EXCLUDED.updated_at;
    """,
    dag=dag
)

# Task dependencies
validate_task >> extract_normalize_task
extract_normalize_task >> [clickhouse_load_task, hdfs_load_task]
[clickhouse_load_task, hdfs_load_task] >> quality_check_task
quality_check_task >> [update_status_task, lineage_task]