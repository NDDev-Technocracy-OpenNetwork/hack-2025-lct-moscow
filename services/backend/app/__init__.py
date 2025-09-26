"""
BigData Processing Backend API

A FastAPI-based backend service for processing large files (up to 10GB)
with support for multiple destinations (PostgreSQL, ClickHouse, HDFS).

Features:
- File upload and analysis
- Real-time WebSocket updates
- DDL/ETL script generation
- Airflow integration for orchestration
- Multi-destination support
"""

__version__ = "1.0.0"