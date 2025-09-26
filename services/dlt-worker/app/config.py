import os
from typing import Dict, Any


class DLTWorkerConfig:
    """Configuration for DLT Worker"""

    # Backend API
    BACKEND_URL = os.getenv('BACKEND_URL', 'http://backend:8000')

    # File paths
    DATA_INPUT_PATH = '/app/data/input'
    DATA_TEMP_PATH = '/app/data/temp'
    DATA_PROCESSED_PATH = '/app/data/processed'
    LOGS_PATH = '/app/logs'

    # Processing settings
    DEFAULT_CHUNK_SIZE = {
        'csv': 10000,
        'json': 10000,
        'xml': 1000  # Smaller for XML due to complexity
    }

    # Database credentials (will be overridden by job config)
    DEFAULT_POSTGRES = {
        'host': 'postgres',
        'port': 5432,
        'username': 'bigdata_user',
        'password': 'bigdata_pass'
    }

    DEFAULT_CLICKHOUSE = {
        'host': 'clickhouse',
        'port': 8123,
        'username': 'bigdata_user',
        'password': 'bigdata_pass'
    }

    DEFAULT_HDFS = {
        'namenode_url': 'http://namenode:9870'
    }

    # Logging configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    @classmethod
    def get_default_destination_config(cls, destination_type: str) -> Dict[str, Any]:
        """Get default configuration for destination type"""
        if destination_type == 'postgresql':
            return cls.DEFAULT_POSTGRES.copy()
        elif destination_type == 'clickhouse':
            return cls.DEFAULT_CLICKHOUSE.copy()
        elif destination_type == 'hdfs':
            return cls.DEFAULT_HDFS.copy()
        else:
            return {}

    @classmethod
    def get_chunk_size(cls, file_format: str, custom_size: int = None) -> int:
        """Get chunk size for file format"""
        if custom_size and custom_size > 0:
            return custom_size
        return cls.DEFAULT_CHUNK_SIZE.get(file_format.lower(), 10000)