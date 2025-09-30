"""
System Monitoring and Health Check DAG - Production
Monitors BigData infrastructure components
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import redis
import psutil
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'bigdata-ops',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 26),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'system_monitoring_health_check',
    default_args=default_args,
    description='Production system monitoring and health checks',
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    tags=['monitoring', 'health', 'production']
)

def check_postgres_health(**context):
    """Check PostgreSQL database health and performance"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id='bigdata_postgres')

        # Check connection
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # Check database size
                cursor.execute("""
                    SELECT pg_size_pretty(pg_database_size('bigdata_db')) as db_size,
                           pg_size_pretty(pg_total_relation_size('processing_jobs')) as jobs_table_size
                """)
                sizes = cursor.fetchone()

                # Check active connections
                cursor.execute("SELECT count(*) FROM pg_stat_activity WHERE state = 'active'")
                active_connections = cursor.fetchone()[0]

                # Check long running queries
                cursor.execute("""
                    SELECT count(*) FROM pg_stat_activity
                    WHERE state = 'active' AND now() - query_start > interval '5 minutes'
                """)
                long_queries = cursor.fetchone()[0]

        metrics = {
            'database_size': sizes[0],
            'jobs_table_size': sizes[1],
            'active_connections': active_connections,
            'long_running_queries': long_queries,
            'status': 'healthy'
        }

        logger.info(f"PostgreSQL health check passed: {metrics}")
        return metrics

    except Exception as e:
        logger.error(f"PostgreSQL health check failed: {e}")
        raise

def check_redis_health(**context):
    """Check Redis cache health and performance"""
    try:
        r = redis.Redis(host='redis', port=6379, db=0)

        # Check connection
        ping_result = r.ping()

        # Get Redis info
        info = r.info()

        metrics = {
            'ping': ping_result,
            'connected_clients': info.get('connected_clients', 0),
            'used_memory_human': info.get('used_memory_human', 'unknown'),
            'keyspace_hits': info.get('keyspace_hits', 0),
            'keyspace_misses': info.get('keyspace_misses', 0),
            'status': 'healthy'
        }

        logger.info(f"Redis health check passed: {metrics}")
        return metrics

    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        raise

def check_clickhouse_health(**context):
    """Check ClickHouse analytics database health"""
    try:
        response = requests.get('http://clickhouse:8123/ping', timeout=10)

        if response.status_code == 200 and response.text.strip() == 'Ok.':
            # Get system metrics
            query_response = requests.get(
                'http://clickhouse:8123',
                params={'query': 'SELECT version(), uptime()'},
                timeout=10
            )

            metrics = {
                'ping_status': 'Ok',
                'http_status': response.status_code,
                'query_response': query_response.text.strip() if query_response.status_code == 200 else 'error',
                'status': 'healthy'
            }
        else:
            raise Exception(f"ClickHouse ping failed: {response.status_code}")

        logger.info(f"ClickHouse health check passed: {metrics}")
        return metrics

    except Exception as e:
        logger.error(f"ClickHouse health check failed: {e}")
        raise

def check_hdfs_health(**context):
    """Check HDFS cluster health"""
    try:
        # Check namenode
        response = requests.get('http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus', timeout=10)

        if response.status_code == 200:
            jmx_data = response.json()

            namenode_info = requests.get('http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState', timeout=10)
            fs_info = namenode_info.json() if namenode_info.status_code == 200 else {}

            metrics = {
                'namenode_status': 'active',
                'http_status': response.status_code,
                'total_files': fs_info.get('beans', [{}])[0].get('FilesTotal', 0) if fs_info.get('beans') else 0,
                'total_blocks': fs_info.get('beans', [{}])[0].get('BlocksTotal', 0) if fs_info.get('beans') else 0,
                'status': 'healthy'
            }
        else:
            raise Exception(f"HDFS namenode check failed: {response.status_code}")

        logger.info(f"HDFS health check passed: {metrics}")
        return metrics

    except Exception as e:
        logger.error(f"HDFS health check failed: {e}")
        raise

def check_backend_api_health(**context):
    """Check BigData Backend API health"""
    try:
        response = requests.get('http://backend:8000/health', timeout=10)

        if response.status_code == 200:
            health_data = response.json()

            # Check jobs endpoint
            jobs_response = requests.get('http://backend:8000/api/v1/jobs/', timeout=10)

            metrics = {
                'api_status': health_data.get('status', 'unknown'),
                'service_name': health_data.get('service', 'unknown'),
                'version': health_data.get('version', 'unknown'),
                'jobs_endpoint_status': jobs_response.status_code,
                'status': 'healthy'
            }
        else:
            raise Exception(f"Backend API check failed: {response.status_code}")

        logger.info(f"Backend API health check passed: {metrics}")
        return metrics

    except Exception as e:
        logger.error(f"Backend API health check failed: {e}")
        raise

def collect_system_metrics(**context):
    """Collect system-level metrics"""
    try:
        # CPU and Memory usage
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')

        metrics = {
            'cpu_percent': cpu_percent,
            'memory_percent': memory.percent,
            'memory_available_gb': round(memory.available / (1024**3), 2),
            'disk_percent': disk.percent,
            'disk_free_gb': round(disk.free / (1024**3), 2),
            'status': 'collected'
        }

        logger.info(f"System metrics collected: {metrics}")
        return metrics

    except Exception as e:
        logger.error(f"System metrics collection failed: {e}")
        raise

def store_health_metrics(**context):
    """Store all health metrics in database"""
    task_instance = context['task_instance']

    # Get metrics from previous tasks
    postgres_metrics = task_instance.xcom_pull(task_ids='check_postgres_health')
    redis_metrics = task_instance.xcom_pull(task_ids='check_redis_health')
    clickhouse_metrics = task_instance.xcom_pull(task_ids='check_clickhouse_health')
    hdfs_metrics = task_instance.xcom_pull(task_ids='check_hdfs_health')
    backend_metrics = task_instance.xcom_pull(task_ids='check_backend_api_health')
    system_metrics = task_instance.xcom_pull(task_ids='collect_system_metrics')

    postgres_hook = PostgresHook(postgres_conn_id='bigdata_postgres')

    # Store metrics
    insert_query = """
    INSERT INTO system_config (config_key, config_value, updated_at)
    VALUES
        ('health_check_postgres', %s, CURRENT_TIMESTAMP),
        ('health_check_redis', %s, CURRENT_TIMESTAMP),
        ('health_check_clickhouse', %s, CURRENT_TIMESTAMP),
        ('health_check_hdfs', %s, CURRENT_TIMESTAMP),
        ('health_check_backend', %s, CURRENT_TIMESTAMP),
        ('system_metrics', %s, CURRENT_TIMESTAMP)
    ON CONFLICT (config_key) DO UPDATE SET
        config_value = EXCLUDED.config_value,
        updated_at = EXCLUDED.updated_at
    """

    postgres_hook.run(insert_query, parameters=[
        str(postgres_metrics),
        str(redis_metrics),
        str(clickhouse_metrics),
        str(hdfs_metrics),
        str(backend_metrics),
        str(system_metrics)
    ])

    logger.info("Health metrics stored successfully")

# Task definitions
postgres_check = PythonOperator(
    task_id='check_postgres_health',
    python_callable=check_postgres_health,
    dag=dag
)

redis_check = PythonOperator(
    task_id='check_redis_health',
    python_callable=check_redis_health,
    dag=dag
)

clickhouse_check = PythonOperator(
    task_id='check_clickhouse_health',
    python_callable=check_clickhouse_health,
    dag=dag
)

hdfs_check = PythonOperator(
    task_id='check_hdfs_health',
    python_callable=check_hdfs_health,
    dag=dag
)

backend_check = PythonOperator(
    task_id='check_backend_api_health',
    python_callable=check_backend_api_health,
    dag=dag
)

system_metrics_task = PythonOperator(
    task_id='collect_system_metrics',
    python_callable=collect_system_metrics,
    dag=dag
)

store_metrics_task = PythonOperator(
    task_id='store_health_metrics',
    python_callable=store_health_metrics,
    dag=dag
)

# Cleanup old logs
cleanup_logs = BashOperator(
    task_id='cleanup_old_logs',
    bash_command="""
    find /opt/airflow/logs -name "*.log" -mtime +7 -delete
    find /var/log/supervisor -name "*.log" -mtime +7 -delete
    echo "Log cleanup completed"
    """,
    dag=dag
)

# Task dependencies - run health checks in parallel, then store results
[postgres_check, redis_check, clickhouse_check, hdfs_check, backend_check, system_metrics_task] >> store_metrics_task
store_metrics_task >> cleanup_logs