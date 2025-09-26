import asyncio
import logging
import json
import os
import sys
import requests
from datetime import datetime
from typing import Dict, Any

from pipelines import CSVPipeline, JSONPipeline, XMLPipeline

# Setup logging
def setup_logging():
    """Setup logging configuration"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Create logs directory if it doesn't exist
    os.makedirs('/app/logs', exist_ok=True)

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler('/app/logs/dlt-worker.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Set specific log levels
    logging.getLogger('dlt').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


class DLTWorker:
    """Main DLT Worker for ETL processing"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.pipeline_classes = {
            'csv': CSVPipeline,
            'json': JSONPipeline,
            'xml': XMLPipeline
        }

        # Backend API configuration
        self.backend_url = os.getenv('BACKEND_URL', 'http://backend:8000')

    async def process_job(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single ETL job"""

        job_id = job_config.get('job_id')
        file_format = job_config.get('file_format', '').lower()

        self.logger.info(f"Starting job {job_id} for {file_format} file: {job_config.get('filename', 'unknown')}")

        # Validate job configuration
        if not self._validate_job_config(job_config):
            return self._create_error_result(job_id, "Invalid job configuration")

        try:
            # Check if pipeline class exists for the format
            if file_format not in self.pipeline_classes:
                raise ValueError(f"Unsupported file format: {file_format}")

            # Create pipeline instance
            pipeline_class = self.pipeline_classes[file_format]
            pipeline = pipeline_class(job_config)

            # Set progress callback to send updates to backend
            pipeline.set_progress_callback(self._create_progress_callback(job_id))

            # Send initial status
            await self._send_job_status(job_id, {
                'status': 'started',
                'stage': 'initializing',
                'message': f'Starting {file_format.upper()} processing pipeline'
            })

            # Run the ETL pipeline
            result = pipeline.run()

            # Send final status
            await self._send_job_status(job_id, {
                'status': 'completed',
                'stage': 'finished',
                'message': f'ETL process completed successfully',
                'records_processed': result.get('records_processed', 0),
                'total_records_loaded': result.get('total_records_loaded', 0)
            })

            self.logger.info(f"Job {job_id} completed successfully. Processed {result.get('records_processed', 0)} records")
            return result

        except Exception as e:
            self.logger.error(f"Job {job_id} failed: {str(e)}", exc_info=True)

            # Send error status
            await self._send_job_status(job_id, {
                'status': 'failed',
                'stage': 'error',
                'error_message': str(e),
                'message': f'ETL process failed: {str(e)}'
            })

            return self._create_error_result(job_id, str(e))

    def _validate_job_config(self, job_config: Dict[str, Any]) -> bool:
        """Validate job configuration"""
        required_fields = ['job_id', 'filename', 'file_format', 'destination_type', 'destination_config']

        for field in required_fields:
            if field not in job_config:
                self.logger.error(f"Missing required field in job config: {field}")
                return False

        # Validate destination config
        destination_config = job_config.get('destination_config', {})
        destination_type = job_config.get('destination_type')

        if destination_type == 'postgresql':
            required_pg_fields = ['database', 'table_name']
            for field in required_pg_fields:
                if field not in destination_config:
                    self.logger.error(f"Missing PostgreSQL config field: {field}")
                    return False

        elif destination_type == 'clickhouse':
            required_ch_fields = ['database', 'table_name']
            for field in required_ch_fields:
                if field not in destination_config:
                    self.logger.error(f"Missing ClickHouse config field: {field}")
                    return False

        elif destination_type == 'hdfs':
            required_hdfs_fields = ['path']
            for field in required_hdfs_fields:
                if field not in destination_config:
                    self.logger.error(f"Missing HDFS config field: {field}")
                    return False

        return True

    def _create_progress_callback(self, job_id: str):
        """Create progress callback function for pipeline"""

        def progress_callback(progress_data: Dict[str, Any]):
            """Send progress update to backend"""
            try:
                # Add job_id if not present
                if 'job_id' not in progress_data:
                    progress_data['job_id'] = job_id

                # Send to backend API
                asyncio.create_task(self._send_progress_update(progress_data))

            except Exception as e:
                self.logger.warning(f"Failed to send progress update: {e}")

        return progress_callback

    async def _send_progress_update(self, progress_data: Dict[str, Any]):
        """Send progress update to backend API"""
        try:
            url = f"{self.backend_url}/api/v1/jobs/callback/airflow"

            # Convert progress data to callback format
            callback_data = {
                'job_id': progress_data.get('job_id'),
                'status': progress_data.get('status', 'processing'),
                'stage': progress_data.get('stage', 'processing'),
                'progress_percent': progress_data.get('progress_percent'),
                'records_processed': progress_data.get('records_processed'),
                'total_records': progress_data.get('total_records'),
                'message': progress_data.get('message')
            }

            response = requests.post(url, json=callback_data, timeout=10)

            if response.status_code == 200:
                self.logger.debug(f"Progress update sent successfully for job {progress_data.get('job_id')}")
            else:
                self.logger.warning(f"Failed to send progress update: HTTP {response.status_code}")

        except Exception as e:
            self.logger.warning(f"Error sending progress update: {e}")

    async def _send_job_status(self, job_id: str, status_data: Dict[str, Any]):
        """Send job status update to backend"""
        try:
            # Send notification via WebSocket notification endpoint
            url = f"{self.backend_url}/api/v1/notify/{job_id}"

            notification = {
                'type': 'job_status',
                'timestamp': datetime.utcnow().isoformat(),
                **status_data
            }

            response = requests.post(url, json=notification, timeout=10)

            if response.status_code == 200:
                self.logger.debug(f"Job status sent successfully for job {job_id}: {status_data.get('status')}")
            else:
                self.logger.warning(f"Failed to send job status: HTTP {response.status_code}")

        except Exception as e:
            self.logger.warning(f"Error sending job status: {e}")

    def _create_error_result(self, job_id: str, error_message: str) -> Dict[str, Any]:
        """Create standardized error result"""
        return {
            'job_id': job_id,
            'status': 'failed',
            'error': error_message,
            'timestamp': datetime.utcnow().isoformat(),
            'records_processed': 0,
            'total_records_loaded': 0
        }


def load_job_config() -> Dict[str, Any]:
    """Load job configuration from environment variable or command line"""

    # Try environment variable first
    job_config_str = os.getenv('DLT_JOB_CONFIG')

    # Try command line argument
    if not job_config_str and len(sys.argv) > 1:
        try:
            with open(sys.argv[1], 'r') as f:
                job_config_str = f.read()
        except Exception as e:
            print(f"Error reading config file {sys.argv[1]}: {e}")
            return None

    # Try standard input
    if not job_config_str:
        try:
            job_config_str = sys.stdin.read()
        except Exception:
            pass

    if not job_config_str:
        print("No job configuration provided. Use DLT_JOB_CONFIG environment variable, file argument, or stdin.")
        return None

    try:
        return json.loads(job_config_str.strip())
    except json.JSONDecodeError as e:
        print(f"Invalid JSON configuration: {e}")
        return None


async def main():
    """Main entry point"""
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("🚀 DLT Worker starting...")

    # Load job configuration
    job_config = load_job_config()
    if not job_config:
        logger.error("Failed to load job configuration")
        sys.exit(1)

    logger.info(f"Loaded job configuration: {job_config.get('job_id', 'unknown')} - {job_config.get('filename', 'unknown')}")

    # Create and run worker
    worker = DLTWorker()

    try:
        result = await worker.process_job(job_config)

        logger.info(f"Job processing result: {result.get('status', 'unknown')}")

        # Exit with appropriate code
        exit_code = 0 if result.get('status') != 'failed' else 1
        logger.info(f"DLT Worker exiting with code {exit_code}")
        sys.exit(exit_code)

    except Exception as e:
        logger.error(f"Worker failed to process job: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    # Run the main function
    asyncio.run(main())