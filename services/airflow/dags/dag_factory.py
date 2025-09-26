"""
Dynamic DAG Factory for BigData Processing System

This module provides functionality to create DAGs dynamically based on job configurations
received from the backend API. It integrates with the FastAPI backend to create
processing pipelines on-demand.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
import logging
import json
from typing import Dict, Any, Optional

from dynamic_etl_dag import create_dynamic_etl_dag

logger = logging.getLogger(__name__)


class DAGFactory:
    """Factory class for creating dynamic DAGs"""

    def __init__(self):
        self.created_dags = {}

    def create_dag_from_job(self, job_config: Dict[str, Any]) -> Optional[DAG]:
        """
        Create a DAG from job configuration

        Args:
            job_config: Job configuration dictionary

        Returns:
            DAG object or None if creation failed
        """
        try:
            job_id = job_config.get('job_id')
            if not job_id:
                logger.error("Job configuration missing job_id")
                return None

            logger.info(f"Creating dynamic DAG for job {job_id}")

            # Validate job configuration
            if not self._validate_job_config(job_config):
                return None

            # Create the DAG
            dag = create_dynamic_etl_dag(job_config)

            # Store reference
            self.created_dags[job_id] = dag

            logger.info(f"Successfully created DAG {dag.dag_id} for job {job_id}")
            return dag

        except Exception as e:
            logger.error(f"Failed to create DAG for job {job_config.get('job_id', 'unknown')}: {str(e)}")
            return None

    def _validate_job_config(self, job_config: Dict[str, Any]) -> bool:
        """Validate job configuration"""

        required_fields = ['job_id', 'filename', 'file_format', 'destination_type', 'destination_config']

        for field in required_fields:
            if field not in job_config:
                logger.error(f"Missing required field in job config: {field}")
                return False

        # Validate file format
        supported_formats = ['csv', 'json', 'xml']
        if job_config['file_format'] not in supported_formats:
            logger.error(f"Unsupported file format: {job_config['file_format']}")
            return False

        # Validate destination type
        supported_destinations = ['postgresql', 'clickhouse', 'hdfs']
        if job_config['destination_type'] not in supported_destinations:
            logger.error(f"Unsupported destination type: {job_config['destination_type']}")
            return False

        return True

    def get_dag(self, job_id: str) -> Optional[DAG]:
        """Get DAG by job ID"""
        return self.created_dags.get(job_id)

    def remove_dag(self, job_id: str) -> bool:
        """Remove DAG from factory"""
        if job_id in self.created_dags:
            del self.created_dags[job_id]
            logger.info(f"Removed DAG for job {job_id}")
            return True
        return False

    def list_dags(self) -> Dict[str, str]:
        """List all created DAGs"""
        return {job_id: dag.dag_id for job_id, dag in self.created_dags.items()}


# Global DAG factory instance
dag_factory = DAGFactory()


def create_dag_from_api(job_config_json: str) -> Optional[DAG]:
    """
    Create DAG from JSON configuration (for API integration)

    Args:
        job_config_json: JSON string with job configuration

    Returns:
        DAG object or None if creation failed
    """
    try:
        job_config = json.loads(job_config_json)
        return dag_factory.create_dag_from_job(job_config)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON configuration: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Failed to create DAG from API: {str(e)}")
        return None


def register_dag_in_globals(dag: DAG):
    """
    Register DAG in global namespace so Airflow can discover it

    Args:
        dag: DAG object to register
    """
    try:
        globals()[dag.dag_id] = dag
        logger.info(f"Registered DAG {dag.dag_id} in globals")
    except Exception as e:
        logger.error(f"Failed to register DAG {dag.dag_id}: {str(e)}")


def create_and_register_dag(job_config: Dict[str, Any]) -> Optional[str]:
    """
    Create and register a DAG in one step

    Args:
        job_config: Job configuration dictionary

    Returns:
        DAG ID if successful, None otherwise
    """
    try:
        dag = dag_factory.create_dag_from_job(job_config)
        if dag:
            register_dag_in_globals(dag)
            return dag.dag_id
        return None
    except Exception as e:
        logger.error(f"Failed to create and register DAG: {str(e)}")
        return None


# Example usage and testing functions
def create_sample_dag():
    """Create a sample DAG for testing"""

    sample_config = {
        'job_id': 'test-job-' + datetime.now().strftime('%Y%m%d-%H%M%S'),
        'filename': 'test_data.csv',
        'file_format': 'csv',
        'destination_type': 'postgresql',
        'destination_config': {
            'host': 'postgres',
            'port': 5432,
            'database': 'bigdata_db',
            'schema_name': 'public',
            'table_name': 'test_data',
            'username': 'bigdata_user',
            'password': 'bigdata_pass'
        }
    }

    return create_and_register_dag(sample_config)


# Auto-create sample DAG in development environment
import os
if os.getenv('AIRFLOW_ENV') == 'development':
    sample_dag_id = create_sample_dag()
    if sample_dag_id:
        logger.info(f"Created sample DAG: {sample_dag_id}")


# Helper functions for backend integration
def trigger_dag_via_api(dag_id: str, job_config: Dict[str, Any]) -> bool:
    """
    Trigger DAG execution via Airflow API

    Args:
        dag_id: DAG ID to trigger
        job_config: Job configuration to pass to DAG

    Returns:
        True if trigger successful, False otherwise
    """
    try:
        from airflow.api.client.local_client import Client

        client = Client(None, None)

        # Trigger DAG run
        run_id = client.trigger_dag(
            dag_id=dag_id,
            conf=job_config,
            execution_date=datetime.now(),
            replace_microseconds=False
        )

        logger.info(f"Triggered DAG {dag_id} with run_id: {run_id}")
        return True

    except Exception as e:
        logger.error(f"Failed to trigger DAG {dag_id}: {str(e)}")
        return False


def get_dag_status(dag_id: str, run_id: str = None) -> Dict[str, Any]:
    """
    Get DAG execution status

    Args:
        dag_id: DAG ID
        run_id: Optional run ID (gets latest if not provided)

    Returns:
        Dictionary with DAG status information
    """
    try:
        from airflow.models import DagRun
        from airflow import settings

        session = settings.Session()

        if run_id:
            dag_run = session.query(DagRun).filter(
                DagRun.dag_id == dag_id,
                DagRun.run_id == run_id
            ).first()
        else:
            dag_run = session.query(DagRun).filter(
                DagRun.dag_id == dag_id
            ).order_by(DagRun.execution_date.desc()).first()

        if not dag_run:
            return {'status': 'not_found'}

        return {
            'status': dag_run.state,
            'execution_date': dag_run.execution_date.isoformat(),
            'start_date': dag_run.start_date.isoformat() if dag_run.start_date else None,
            'end_date': dag_run.end_date.isoformat() if dag_run.end_date else None,
            'run_id': dag_run.run_id
        }

    except Exception as e:
        logger.error(f"Failed to get DAG status for {dag_id}: {str(e)}")
        return {'status': 'error', 'error': str(e)}