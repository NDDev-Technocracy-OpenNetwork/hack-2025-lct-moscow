from typing import Dict, Any, List
import re


def validate_destination_config(destination_type: str, config: Dict[str, Any]) -> List[str]:
    """Validate destination configuration and return list of errors"""

    errors = []

    if destination_type == "postgresql":
        errors.extend(_validate_postgresql_config(config))
    elif destination_type == "clickhouse":
        errors.extend(_validate_clickhouse_config(config))
    elif destination_type == "hdfs":
        errors.extend(_validate_hdfs_config(config))
    else:
        errors.append(f"Unsupported destination type: {destination_type}")

    return errors


def _validate_postgresql_config(config: Dict[str, Any]) -> List[str]:
    """Validate PostgreSQL configuration"""
    errors = []

    required_fields = ["host", "port", "database", "username", "password", "table_name"]
    for field in required_fields:
        if field not in config or not config[field]:
            errors.append(f"Missing required field: {field}")

    # Validate port
    if "port" in config:
        try:
            port = int(config["port"])
            if not (1 <= port <= 65535):
                errors.append("Port must be between 1 and 65535")
        except (ValueError, TypeError):
            errors.append("Port must be a valid integer")

    # Validate table name
    if "table_name" in config and config["table_name"]:
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', config["table_name"]):
            errors.append("Table name must be a valid SQL identifier")

    return errors


def _validate_clickhouse_config(config: Dict[str, Any]) -> List[str]:
    """Validate ClickHouse configuration"""
    errors = []

    required_fields = ["host", "port", "database", "username", "password", "table_name"]
    for field in required_fields:
        if field not in config or not config[field]:
            errors.append(f"Missing required field: {field}")

    # Validate port
    if "port" in config:
        try:
            port = int(config["port"])
            if not (1 <= port <= 65535):
                errors.append("Port must be between 1 and 65535")
        except (ValueError, TypeError):
            errors.append("Port must be a valid integer")

    # Validate engine
    valid_engines = ["MergeTree", "ReplacingMergeTree", "CollapsingMergeTree", "Log", "TinyLog"]
    if "engine" in config and config["engine"] not in valid_engines:
        errors.append(f"Engine must be one of: {', '.join(valid_engines)}")

    return errors


def _validate_hdfs_config(config: Dict[str, Any]) -> List[str]:
    """Validate HDFS configuration"""
    errors = []

    required_fields = ["path"]
    for field in required_fields:
        if field not in config or not config[field]:
            errors.append(f"Missing required field: {field}")

    # Validate path format
    if "path" in config and config["path"]:
        path = config["path"]
        if not path.startswith("/"):
            errors.append("HDFS path must start with '/'")

    # Validate file format
    valid_formats = ["parquet", "csv", "json"]
    if "file_format" in config and config["file_format"] not in valid_formats:
        errors.append(f"File format must be one of: {', '.join(valid_formats)}")

    return errors