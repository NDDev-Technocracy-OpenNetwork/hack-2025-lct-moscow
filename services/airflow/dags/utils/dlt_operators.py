from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
import subprocess
import json
import logging
import os
import tempfile
from typing import Dict, Any

logger = logging.getLogger(__name__)


class DltETLOperator(BaseOperator):
    """
    Operator for executing ETL processes using dlt worker
    """

    def __init__(self, job_config: Dict[str, Any], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_config = job_config

    def execute(self, context: Context):
        """Execute ETL process using dlt worker"""

        job_id = self.job_config['job_id']
        logger.info(f"Starting DLT ETL process for job {job_id}")

        try:
            # Update job status to processing
            self._update_job_status('processing', 'Starting DLT worker')

            # Create temporary config file for dlt worker
            config_file = self._create_worker_config()

            # Execute dlt worker
            result = self._run_dlt_worker(config_file)

            # Update job with results
            self._update_job_progress(result)

            logger.info(f"DLT ETL process completed successfully for job {job_id}")
            return result

        except Exception as e:
            logger.error(f"DLT ETL process failed for job {job_id}: {str(e)}")
            self._update_job_status('failed', str(e))
            raise
        finally:
            # Cleanup temporary files
            if 'config_file' in locals():
                try:
                    os.unlink(config_file)
                except:
                    pass

    def _create_worker_config(self) -> str:
        """Create configuration file for dlt worker"""

        # Enhanced configuration with additional settings
        worker_config = {
            'job_id': self.job_config['job_id'],
            'filename': self.job_config['filename'],
            'file_format': self.job_config['file_format'],
            'destination_type': self.job_config['destination_type'],
            'destination_config': self.job_config['destination_config'],
            'chunk_size': self._get_chunk_size(),
            'max_workers': self._get_max_workers(),
            'timeout': 7200,  # 2 hours timeout
        }

        # Merge with default destination configs
        worker_config['destination_config'] = self._merge_destination_config(
            worker_config['destination_config']
        )

        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(worker_config, f, indent=2)
            config_file = f.name

        logger.info(f"Created worker config file: {config_file}")
        return config_file

    def _get_chunk_size(self) -> int:
        """Get appropriate chunk size based on file format"""
        format_chunk_sizes = {
            'csv': 10000,
            'json': 10000,
            'xml': 1000  # Smaller for XML due to complexity
        }
        return format_chunk_sizes.get(self.job_config['file_format'], 10000)

    def _get_max_workers(self) -> int:
        """Get number of worker processes based on destination"""
        # Conservative settings for database connections
        if self.job_config['destination_type'] in ['postgresql', 'clickhouse']:
            return 2
        return 4

    def _merge_destination_config(self, dest_config: Dict[str, Any]) -> Dict[str, Any]:
        """Merge destination config with defaults"""

        # Default configurations for each destination
        defaults = {
            'postgresql': {
                'host': 'postgres',
                'port': 5432,
                'username': 'bigdata_user',
                'password': 'bigdata_pass',
                'database': 'bigdata_db',
                'schema_name': 'public'
            },
            'clickhouse': {
                'host': 'clickhouse',
                'port': 8123,
                'username': 'bigdata_user',
                'password': 'bigdata_pass',
                'database': 'analytics'
            },
            'hdfs': {
                'namenode_url': 'http://namenode:9870',
                'path': '/bigdata/processed',
                'file_format': 'parquet',
                'compression': 'gzip'
            }
        }

        destination_type = self.job_config['destination_type']
        merged_config = defaults.get(destination_type, {}).copy()
        merged_config.update(dest_config)

        return merged_config

    def _run_dlt_worker(self, config_file: str) -> Dict[str, Any]:
        """Run dlt worker process"""

        # Command to run dlt worker
        cmd = [
            'python', '/app/services/dlt-worker/app/main.py',
            config_file
        ]

        logger.info(f"Executing dlt worker: {' '.join(cmd)}")

        try:
            # Run dlt worker with timeout
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=7200,  # 2 hour timeout
                cwd='/app'
            )

            # Check return code
            if result.returncode != 0:
                error_msg = f"DLT worker failed with exit code {result.returncode}"
                if result.stderr:
                    error_msg += f": {result.stderr}"
                logger.error(error_msg)
                logger.error(f"Worker stdout: {result.stdout}")
                raise RuntimeError(error_msg)

            # Parse output for result information
            worker_result = self._parse_worker_output(result.stdout)

            logger.info(f"DLT worker completed successfully")
            logger.debug(f"Worker output: {result.stdout}")

            return worker_result

        except subprocess.TimeoutExpired:
            error_msg = "DLT worker timed out after 2 hours"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        except Exception as e:
            error_msg = f"Failed to execute DLT worker: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    def _parse_worker_output(self, output: str) -> Dict[str, Any]:
        """Parse dlt worker output for result information"""

        result = {
            'status': 'completed',
            'records_processed': 0,
            'message': 'DLT processing completed'
        }

        # Try to extract information from output
        lines = output.split('\n')
        for line in lines:
            if 'records processed' in line.lower():
                try:
                    # Extract number from line like "Processed 12345 records"
                    import re
                    match = re.search(r'(\d+)\s+records', line)
                    if match:
                        result['records_processed'] = int(match.group(1))
                except:
                    pass

            elif 'error' in line.lower() or 'failed' in line.lower():
                result['status'] = 'failed'
                result['message'] = line.strip()
                break

        return result

    def _update_job_status(self, status: str, message: str = None):
        """Update job status in database"""
        try:
            pg_hook = PostgresHook(postgres_conn_id='bigdata_postgres')

            if message:
                pg_hook.run(
                    """UPDATE processing_jobs
                       SET status = %s, error_message = %s, updated_at = CURRENT_TIMESTAMP
                       WHERE job_id = %s""",
                    parameters=[status, message, self.job_config['job_id']]
                )
            else:
                pg_hook.run(
                    """UPDATE processing_jobs
                       SET status = %s, updated_at = CURRENT_TIMESTAMP
                       WHERE job_id = %s""",
                    parameters=[status, self.job_config['job_id']]
                )

        except Exception as e:
            logger.warning(f"Failed to update job status: {str(e)}")

    def _update_job_progress(self, result: Dict[str, Any]):
        """Update job progress with final results"""
        try:
            pg_hook = PostgresHook(postgres_conn_id='bigdata_postgres')

            pg_hook.run(
                """UPDATE processing_jobs
                   SET records_processed = %s,
                       status = %s,
                       updated_at = CURRENT_TIMESTAMP
                   WHERE job_id = %s""",
                parameters=[
                    result.get('records_processed', 0),
                    result.get('status', 'completed'),
                    self.job_config['job_id']
                ]
            )

            logger.info(f"Updated job progress: {result.get('records_processed', 0)} records processed")

        except Exception as e:
            logger.warning(f"Failed to update job progress: {str(e)}")


class DltConnectionTestOperator(BaseOperator):
    """
    Operator for testing destination connections before processing
    """

    def __init__(self, job_config: Dict[str, Any], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_config = job_config

    def execute(self, context: Context):
        """Test connection to destination"""

        destination_type = self.job_config['destination_type']
        destination_config = self.job_config['destination_config']

        logger.info(f"Testing {destination_type} connection")

        try:
            if destination_type == 'postgresql':
                self._test_postgres_connection(destination_config)
            elif destination_type == 'clickhouse':
                self._test_clickhouse_connection(destination_config)
            elif destination_type == 'hdfs':
                self._test_hdfs_connection(destination_config)
            else:
                raise ValueError(f"Unsupported destination type: {destination_type}")

            logger.info(f"Connection test successful for {destination_type}")
            return True

        except Exception as e:
            error_msg = f"Connection test failed for {destination_type}: {str(e)}"
            logger.error(error_msg)
            raise

    def _test_postgres_connection(self, config: Dict[str, Any]):
        """Test PostgreSQL connection"""
        import psycopg2

        conn = psycopg2.connect(
            host=config.get('host', 'postgres'),
            port=config.get('port', 5432),
            database=config.get('database', 'bigdata_db'),
            user=config.get('username', 'bigdata_user'),
            password=config.get('password', 'bigdata_pass')
        )
        conn.close()

    def _test_clickhouse_connection(self, config: Dict[str, Any]):
        """Test ClickHouse connection"""
        from clickhouse_driver import Client

        client = Client(
            host=config.get('host', 'clickhouse'),
            port=config.get('port', 9000),
            user=config.get('username', 'bigdata_user'),
            password=config.get('password', 'bigdata_pass'),
            database=config.get('database', 'analytics')
        )
        client.execute('SELECT 1')

    def _test_hdfs_connection(self, config: Dict[str, Any]):
        """Test HDFS connection"""
        import requests

        namenode_url = config.get('namenode_url', 'http://namenode:9870')
        health_url = f"{namenode_url}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus"

        response = requests.get(health_url, timeout=10)
        response.raise_for_status()