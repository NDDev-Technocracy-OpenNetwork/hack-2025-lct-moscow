from pydantic_settings import BaseSettings
from typing import Optional, List
import os


class Settings(BaseSettings):
    # Database
    DATABASE_URL: str

    # Redis
    REDIS_URL: str = "redis://redis:6379"

    # Airflow
    AIRFLOW_BASE_URL: str = "http://airflow:8080"
    AIRFLOW_USERNAME: str = "admin"
    AIRFLOW_PASSWORD: str

    # ClickHouse
    CLICKHOUSE_URL: str = "http://clickhouse:8123"
    CLICKHOUSE_USER: str = "bigdata_user"
    CLICKHOUSE_PASSWORD: str
    CLICKHOUSE_DATABASE: str = "analytics"

    # HDFS
    HDFS_NAMENODE_URL: str = "http://namenode:9870"
    HDFS_USER: str = "root"

    # File processing
    MAX_FILE_SIZE: int = 10 * 1024 * 1024 * 1024  # 10GB
    ALLOWED_FILE_FORMATS: List[str] = ["csv", "json", "xml"]

    # Paths
    DATA_INPUT_PATH: str = "/app/data/input"
    DATA_TEMP_PATH: str = "/app/data/temp"
    DATA_PROCESSED_PATH: str = "/app/data/processed"

    # API
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Data Processing API"

    # CORS origins from environment variable
    BACKEND_CORS_ORIGINS: str = os.getenv("BACKEND_CORS_ORIGINS", "http://frontend:3000")

    @property
    def cors_origins_list(self) -> List[str]:
        """Convert comma-separated CORS origins string to list"""
        return [origin.strip() for origin in self.BACKEND_CORS_ORIGINS.split(",")]

    class Config:
        env_file = ".env"


settings = Settings()