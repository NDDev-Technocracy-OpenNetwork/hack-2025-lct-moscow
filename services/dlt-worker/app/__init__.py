"""
DLT Worker Application

A specialized worker for processing large files (CSV, JSON, XML) up to 10GB
using the dlt (Data Load Tool) library with support for multiple destinations.

Features:
- Streaming file processing for memory efficiency
- Support for PostgreSQL, ClickHouse, and HDFS destinations
- Real-time progress tracking via HTTP callbacks
- Automatic data type inference and cleaning
- Configurable chunk sizes for different file formats
"""

__version__ = "1.0.0"