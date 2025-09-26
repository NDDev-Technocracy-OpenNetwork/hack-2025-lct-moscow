import dlt
from abc import ABC, abstractmethod
from typing import Any, Dict, Generator, Optional, Callable
import logging
from datetime import datetime


class BasePipeline(ABC):
    """Base class for all ETL pipelines"""

    def __init__(self, job_config: Dict[str, Any]):
        self.job_config = job_config
        self.job_id = job_config['job_id']
        self.filename = job_config.get('filename', '')
        self.file_format = job_config.get('file_format', 'csv')
        self.destination_type = job_config.get('destination_type', 'postgresql')
        self.destination_config = job_config.get('destination_config', {})

        self.logger = logging.getLogger(f"pipeline.{self.job_id}")
        self.progress_callback: Optional[Callable] = None
        self.records_processed = 0
        self.total_records = 0

    def set_progress_callback(self, callback: Callable):
        """Set callback for progress tracking"""
        self.progress_callback = callback

    def update_progress(self, records_count: int, stage: str = "processing", message: str = None):
        """Update processing progress"""
        self.records_processed += records_count

        progress_data = {
            'job_id': self.job_id,
            'stage': stage,
            'status': 'processing',
            'records_processed': self.records_processed,
            'total_records': self.total_records,
            'progress_percent': round((self.records_processed / max(self.total_records, 1)) * 100, 2) if self.total_records > 0 else 0,
            'message': message or f"Processed {records_count} records",
            'timestamp': datetime.utcnow().isoformat()
        }

        if self.progress_callback:
            try:
                self.progress_callback(progress_data)
            except Exception as e:
                self.logger.warning(f"Progress callback failed: {e}")

        self.logger.info(f"Progress: {stage} - {self.records_processed}/{self.total_records} records")

    @abstractmethod
    def create_pipeline(self) -> dlt.Pipeline:
        """Create dlt pipeline with proper configuration"""
        pass

    @abstractmethod
    def create_resource(self) -> Any:
        """Create dlt resource for reading data"""
        pass

    def _create_pipeline_by_destination(self) -> dlt.Pipeline:
        """Helper method to create pipeline based on destination type"""

        pipeline_name = f"{self.file_format}_job_{self.job_id}"

        if self.destination_type == 'postgresql':
            return dlt.pipeline(
                pipeline_name=pipeline_name,
                destination='postgres',
                dataset_name=self.destination_config.get('schema_name', 'public'),
                credentials={
                    'host': self.destination_config.get('host', 'postgres'),
                    'port': self.destination_config.get('port', 5432),
                    'database': self.destination_config.get('database', 'bigdata_db'),
                    'username': self.destination_config.get('username', 'bigdata_user'),
                    'password': self.destination_config.get('password', 'bigdata_pass')
                }
            )

        elif self.destination_type == 'clickhouse':
            return dlt.pipeline(
                pipeline_name=pipeline_name,
                destination='clickhouse',
                dataset_name=self.destination_config.get('database', 'analytics'),
                credentials={
                    'host': self.destination_config.get('host', 'clickhouse'),
                    'port': self.destination_config.get('port', 8123),
                    'username': self.destination_config.get('username', 'bigdata_user'),
                    'password': self.destination_config.get('password', 'bigdata_pass'),
                    'database': self.destination_config.get('database', 'analytics')
                }
            )

        elif self.destination_type == 'hdfs':
            return dlt.pipeline(
                pipeline_name=pipeline_name,
                destination='filesystem',
                credentials={
                    'bucket_url': f"file://{self.destination_config.get('path', '/app/data/processed')}"
                },
                dataset_name='hdfs_data'
            )

        else:
            raise ValueError(f"Unsupported destination type: {self.destination_type}")

    def run(self) -> Dict[str, Any]:
        """Main method for executing ETL process"""
        try:
            self.logger.info(f"Starting ETL process for job {self.job_id}")
            self.update_progress(0, "initializing", "Starting ETL process")

            # Create pipeline
            pipeline = self.create_pipeline()
            self.update_progress(0, "pipeline_created", "Pipeline configuration created")

            # Create resource
            resource = self.create_resource()
            self.update_progress(0, "resource_created", "Data resource created")

            # Execute ETL
            self.update_progress(0, "executing", "Starting data load")
            load_info = pipeline.run(resource, table_name=self.destination_config.get('table_name', 'processed_data'))

            # Calculate final metrics
            total_rows_loaded = 0
            if hasattr(load_info, 'load_packages'):
                for package in load_info.load_packages:
                    if hasattr(package, 'jobs') and package.jobs:
                        for job in package.jobs:
                            if hasattr(job, 'job_file_info') and job.job_file_info:
                                total_rows_loaded += job.job_file_info.rows_count or 0

            self.logger.info(f"ETL process completed. Loaded {total_rows_loaded} records")
            self.update_progress(0, "completed", f"ETL completed successfully. Total records: {total_rows_loaded}")

            return {
                'job_id': self.job_id,
                'status': 'completed',
                'records_processed': self.records_processed,
                'total_records_loaded': total_rows_loaded,
                'destination_type': self.destination_type,
                'pipeline_name': pipeline.pipeline_name,
                'load_info': str(load_info),
                'timestamp': datetime.utcnow().isoformat()
            }

        except Exception as e:
            self.logger.error(f"ETL process failed: {str(e)}", exc_info=True)
            self.update_progress(0, "failed", f"ETL process failed: {str(e)}")
            raise

    def estimate_total_records(self, file_path: str) -> int:
        """Estimate total number of records in file (to be overridden by subclasses)"""
        return 0