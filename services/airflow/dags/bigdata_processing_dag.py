from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'bigdata-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'bigdata_processing',
    default_args=default_args,
    description='BigData file processing pipeline',
    schedule_interval=timedelta(hours=1),
    catchup=False
)

def health_check():
    """Simple health check function"""
    import datetime
    print(f"Airflow DAG health check at {datetime.datetime.now()}")
    return "healthy"

health_check_task = PythonOperator(
    task_id='health_check',
    python_callable=health_check,
    dag=dag
)

system_status = BashOperator(
    task_id='system_status',
    bash_command='echo "BigData processing system is operational"',
    dag=dag
)

health_check_task >> system_status