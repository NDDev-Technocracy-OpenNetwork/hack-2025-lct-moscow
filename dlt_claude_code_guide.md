# dlt Data Pipeline Guide for Claude Code

A comprehensive guide for building data pipelines with dlt (Data Load Tool) covering REST APIs, files (CSV/JSON/XML), SQL databases, and Kafka, with routing to PostgreSQL, ClickHouse, and HDFS.

## Quick Reference

**dlt** is an open-source Python ELT/ETL library that handles schema management, normalization, and incremental loading automatically. It supports 30+ SQL databases, REST APIs, filesystem sources, and Kafka.

## 1. Setup & Installation

### Prerequisites
- Python ≥ 3.9
- PostgreSQL instance (local or managed)
- ClickHouse instance (local, Cloud, or Docker)
- HDFS with WebHDFS endpoint
- Kafka cluster (optional)

### Installation

```bash
# Base installation
pip install dlt

# Destination-specific extras
pip install "dlt[postgres]"      # PostgreSQL
pip install "dlt[clickhouse]"    # ClickHouse
pip install "dlt[filesystem]"    # Filesystem/HDFS via fsspec

# Source extras
pip install confluent-kafka      # Kafka support

# Helper libraries
pip install xmltodict pandas pyarrow clickhouse-connect clickhouse-driver
```

### Project Structure

```
project/
├── .dlt/
│   ├── config.toml
│   └── secrets.toml
├── pipelines/
│   ├── rest_to_any.py
│   ├── files_to_any.py
│   ├── sql_to_any.py
│   ├── kafka_to_any.py
│   └── router.py
└── README.md
```

### Configuration Files

#### `.dlt/secrets.toml`

```toml
# PostgreSQL
[destination.postgres.credentials]
database = "dlt_data"
username = "loader"
password = "your_password"
host = "127.0.0.1"
port = 5432
connect_timeout = 15

# ClickHouse
[destination.clickhouse.credentials]
host = "127.0.0.1"
port = 8123
username = "default"
password = "your_password"

# Filesystem (HDFS via WebHDFS)
[destination.filesystem]
bucket_url = "webhdfs://namenode:50070/user/dlt/landing"

[destination.filesystem.kwargs]
kerberos = false
user = "hdfs"

# Kafka
[sources.kafka.credentials]
bootstrap_servers = "localhost:9092"
group_id = "dlt_group"
security_protocol = "SASL_SSL"
sasl_mechanisms = "PLAIN"
sasl_username = "your_username"
sasl_password = "your_password"
```

#### `.dlt/config.toml`

```toml
[normalize.data_writer]
disable_compression = false

[pipeline]
dataset_name = "default_ds"
```

## 2. Source Implementations

### REST API Source

```python
import dlt
from dlt.sources.rest_api import rest_api_source

def create_rest_source(base_url: str, resources: list, params: dict = None):
    """Create REST API source with pagination and incremental loading"""
    config = {
        "client": {"base_url": base_url},
        "resource_defaults": {
            "endpoint": {"params": params or {}},
            "write_disposition": "merge",
            "primary_key": "id",
        },
        "resources": resources,
    }
    return rest_api_source(config)

# Example: GitHub API with incremental loading
def github_issues_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="github_issues",
        destination="postgres",
        dataset_name="github_data",
    )

    source = rest_api_source({
        "client": {"base_url": "https://api.github.com/repos/dlt-hub/dlt/"},
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
            "endpoint": {"params": {"per_page": 100}},
        },
        "resources": [{
            "name": "issues",
            "endpoint": {
                "path": "issues",
                "params": {
                    "sort": "updated",
                    "direction": "desc",
                    "state": "open",
                    "since": {
                        "type": "incremental",
                        "cursor_path": "updated_at",
                        "initial_value": "2024-01-01T00:00:00Z",
                    },
                },
            },
        }],
    })

    info = pipeline.run(source)
    return info

if __name__ == "__main__":
    print(github_issues_pipeline())
```

### File Sources (CSV, JSON, Parquet, XML)

```python
import dlt
from dlt.sources.filesystem import filesystem, read_csv, read_parquet, read_jsonl
from typing import Iterator
from dlt.common.storages.fsspec_filesystem import FileItemDict

# Standard file formats
def create_file_sources(bucket_url: str, file_glob: str = "**/*"):
    """Create sources for standard file formats"""
    files = filesystem(bucket_url=bucket_url, file_glob=file_glob)
    
    csv_resource = (files | read_csv()).with_name("csv_data")
    parquet_resource = (files | read_parquet()).with_name("parquet_data")
    jsonl_resource = (files | read_jsonl()).with_name("jsonl_data")
    
    return [csv_resource, parquet_resource, jsonl_resource]

# XML file transformer
@dlt.transformer
def read_xml(items: Iterator[FileItemDict]) -> Iterator[dict]:
    """Custom XML reader using xmltodict"""
    import xmltodict
    for file_obj in items:
        with file_obj.open() as f:
            content = f.read()
            if isinstance(content, bytes):
                content = content.decode('utf-8')
            doc = xmltodict.parse(content)
            yield doc

def xml_files_source(bucket_url: str, file_glob: str = "**/*.xml"):
    """Create XML file source"""
    return filesystem(bucket_url=bucket_url, file_glob=file_glob) | read_xml()

# Example pipeline for files
def files_to_hdfs_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="files_to_hdfs",
        destination="filesystem",
        dataset_name="files_landing"
    )
    
    # Process multiple file types
    sources = create_file_sources("webhdfs://namenode:50070/user/dlt/incoming")
    xml_source = xml_files_source("webhdfs://namenode:50070/user/dlt/xml")
    
    info = pipeline.run([*sources, xml_source], write_disposition="append")
    return info
```

### SQL Database Source

```python
import dlt
from dlt.sources.sql_database import sql_database, sql_table
from dlt.sources.incremental import incremental
import pendulum

def create_sql_source(credentials: str, tables: list = None):
    """Create full database source"""
    return sql_database(credentials=credentials, table_names=tables)

def create_incremental_table(credentials: str, table: str, cursor_column: str, 
                           initial_value: str, primary_key: str = "id"):
    """Create incremental table source"""
    inc = incremental(cursor_column, initial_value=pendulum.parse(initial_value))
    return sql_table(
        credentials=credentials,
        table=table,
        incremental=inc,
        write_disposition="merge",
        primary_key=primary_key
    )

# Example: PostgreSQL to ClickHouse pipeline
def postgres_to_clickhouse_pipeline():
    source_credentials = "postgresql://user:pass@localhost/source_db"
    
    pipeline = dlt.pipeline(
        pipeline_name="pg_to_ch",
        destination="clickhouse",
        dataset_name="replicated_data"
    )
    
    # Full database sync
    full_sync = create_sql_source(source_credentials, ["customers", "products"])
    
    # Incremental orders table
    orders_incremental = create_incremental_table(
        credentials=source_credentials,
        table="orders",
        cursor_column="updated_at",
        initial_value="2024-01-01T00:00:00Z"
    )
    
    info = pipeline.run([full_sync, orders_incremental])
    return info
```

### Kafka Source

```python
import dlt
from dlt.sources.kafka import kafka_consumer
from typing import Dict, Any

def custom_message_processor(msg) -> Dict[str, Any]:
    """Custom Kafka message processor"""
    return {
        "_kafka_meta": {
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "timestamp": msg.timestamp()[1] if msg.timestamp()[1] else None
        },
        "key": (msg.key() or b"").decode("utf-8", "ignore"),
        "value": (msg.value() or b"").decode("utf-8", "ignore"),
    }

def kafka_to_clickhouse_pipeline():
    """Kafka events to ClickHouse pipeline"""
    pipeline = dlt.pipeline(
        pipeline_name="kafka_events",
        destination="clickhouse",
        dataset_name="event_stream"
    )
    
    # Basic consumer
    source = kafka_consumer(["events", "metrics"])
    
    # With custom processor
    # source = kafka_consumer(["events"], msg_processor=custom_message_processor)
    
    info = pipeline.run(source, write_disposition="append")
    return info
```

## 3. Destination Configurations

### PostgreSQL

```python
import dlt

def setup_postgres_pipeline(pipeline_name: str, dataset_name: str):
    """Setup PostgreSQL pipeline"""
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="postgres",
        dataset_name=dataset_name,
    )

# Performance tips for PostgreSQL:
# - Use Arrow→CSV COPY for fast bulk loads
# - Configure timestamp precision via column hints
# - Use merge write_disposition for upserts
```

### ClickHouse

```python
import dlt

def setup_clickhouse_pipeline(pipeline_name: str, dataset_name: str):
    """Setup ClickHouse pipeline"""
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="clickhouse",
        dataset_name=dataset_name,
    )

# ClickHouse-specific configurations in .dlt/secrets.toml:
# [destination.clickhouse]
# table_engine_type = "MergeTree"  # or ReplacingMergeTree, AggregatingMergeTree

# Notes:
# - Prefers JSONL/Parquet formats
# - object, time, binary types map to text
# - Dataset names prefix table names
# - Use append write_disposition for event streams
```

### HDFS via Filesystem

```python
import dlt

def setup_hdfs_pipeline(pipeline_name: str, dataset_name: str):
    """Setup HDFS pipeline via filesystem destination"""
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="filesystem",
        dataset_name=dataset_name,
    )

# Usage example
def data_to_hdfs():
    pipeline = setup_hdfs_pipeline("to_hdfs", "data_lake")
    
    # Specify file format for HDFS landing
    info = pipeline.run(source, loader_file_format="jsonl")  # or "parquet"
    return info
```

## 4. Smart Destination Router

```python
from __future__ import annotations
import dlt
from dataclasses import dataclass
from typing import Literal

Destination = Literal["postgres", "clickhouse", "filesystem"]

@dataclass
class DataProfile:
    """Profile for automatic destination selection"""
    approx_rows: int
    approx_row_bytes: int
    is_event_stream: bool
    update_rate: Literal["append_only", "occasional_updates", "frequent_updates"]
    nesting_depth: int
    file_like: bool
    intended_use: Literal["analytics", "oltp", "archive"]

def choose_destination(profile: DataProfile) -> Destination:
    """Smart destination selection based on data profile"""
    
    # Large volume or event stream → ClickHouse
    is_large = (profile.approx_rows >= 5_000_000) or (profile.approx_row_bytes >= 4096)
    
    if profile.intended_use == "analytics" and (profile.is_event_stream or is_large):
        return "clickhouse"
    
    # Transactional/OLTP patterns → PostgreSQL
    if profile.intended_use == "oltp" or profile.update_rate == "frequent_updates":
        return "postgres"
    
    # Raw files, archives, or deeply nested data → HDFS
    if profile.file_like or profile.intended_use == "archive" or profile.nesting_depth > 6:
        return "filesystem"
    
    # Default routing
    return "postgres" if profile.update_rate != "append_only" else "clickhouse"

def run_with_smart_routing(source, dataset_name: str, profile: DataProfile):
    """Run pipeline with automatic destination selection"""
    destination = choose_destination(profile)
    
    pipeline = dlt.pipeline(
        pipeline_name=f"{dataset_name}__{destination}",
        destination=destination,
        dataset_name=dataset_name if destination != "filesystem" else f"{dataset_name}_lake",
    )
    
    write_disposition = "append" if profile.is_event_stream else "merge"
    info = pipeline.run(source, write_disposition=write_disposition)
    
    print(f"Routed to: {destination}")
    return info

# Example usage
def smart_routing_example():
    """Example of smart destination routing"""
    
    # Profile for event stream data
    event_profile = DataProfile(
        approx_rows=10_000_000,
        approx_row_bytes=500,
        is_event_stream=True,
        update_rate="append_only",
        nesting_depth=2,
        file_like=False,
        intended_use="analytics"
    )
    
    # This will route to ClickHouse
    kafka_source = kafka_consumer(["events"])
    return run_with_smart_routing(kafka_source, "event_analytics", event_profile)
```

## 5. Advanced Patterns and Best Practices

### Column Hints and Schema Control

```python
import dlt

@dlt.resource(
    primary_key="id",
    columns={
        "event_timestamp": {
            "data_type": "timestamp",
            "precision": 3,
            "timezone": False
        },
        "amount": {
            "data_type": "decimal",
            "precision": 10,
            "scale": 2
        }
    }
)
def events_with_hints():
    """Resource with explicit column hints"""
    yield [
        {
            "id": 1,
            "event_timestamp": "2025-01-01T00:00:00.123",
            "amount": "99.99"
        }
    ]
```

### Pipeline Monitoring and Inspection

```python
def inspect_pipeline_run(pipeline, info):
    """Inspect pipeline run results"""
    print("Pipeline info:", info)
    
    # Detailed normalization info
    if pipeline.last_trace:
        normalize_info = pipeline.last_trace.last_normalize_info
        print("Schema info:", normalize_info)
    
    # Show pipeline schema
    print("Current schema:", pipeline.default_schema.to_pretty_yaml())

# Usage
def monitored_pipeline():
    pipeline = dlt.pipeline("monitored", destination="postgres", dataset_name="test")
    source = rest_api_source(config)
    info = pipeline.run(source)
    
    inspect_pipeline_run(pipeline, info)
    return info
```

### Error Handling and Retry Logic

```python
import time
from dlt.common.exceptions import PipelineException

def robust_pipeline_run(pipeline, source, max_retries=3):
    """Pipeline with retry logic"""
    for attempt in range(max_retries):
        try:
            info = pipeline.run(source)
            print(f"Pipeline successful on attempt {attempt + 1}")
            return info
        except PipelineException as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)  # Exponential backoff
```

## 6. Complete Pipeline Examples

### E-commerce Data Pipeline

```python
import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.sources.sql_database import sql_database

def ecommerce_pipeline():
    """Complete e-commerce data pipeline"""
    
    # API data (orders, customers)
    api_source = rest_api_source({
        "client": {"base_url": "https://api.ecommerce.com/v1/"},
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
        },
        "resources": [
            {
                "name": "orders",
                "endpoint": {
                    "path": "orders",
                    "params": {
                        "updated_since": {
                            "type": "incremental",
                            "cursor_path": "updated_at",
                            "initial_value": "2024-01-01T00:00:00Z",
                        }
                    }
                }
            },
            {"name": "customers", "endpoint": {"path": "customers"}},
            {"name": "products", "endpoint": {"path": "products"}}
        ]
    })
    
    # Database source (inventory, analytics)
    db_source = sql_database(
        credentials="postgresql://user:pass@localhost/inventory",
        table_names=["stock_levels", "supplier_data"]
    )
    
    # Route to appropriate destinations
    # Transactional data → PostgreSQL
    transactional_pipeline = dlt.pipeline(
        "ecommerce_transactional",
        destination="postgres",
        dataset_name="ecommerce"
    )
    
    # Analytics data → ClickHouse
    analytics_pipeline = dlt.pipeline(
        "ecommerce_analytics",
        destination="clickhouse",
        dataset_name="ecommerce_analytics"
    )
    
    # Run pipelines
    tx_info = transactional_pipeline.run([api_source, db_source])
    analytics_info = analytics_pipeline.run(api_source, write_disposition="append")
    
    return {"transactional": tx_info, "analytics": analytics_info}
```

### Log Processing Pipeline

```python
import dlt
from dlt.sources.filesystem import filesystem, read_jsonl
from dlt.sources.kafka import kafka_consumer

def log_processing_pipeline():
    """Process logs from multiple sources"""
    
    # Historical logs from HDFS
    historical_logs = filesystem(
        bucket_url="webhdfs://namenode:50070/user/logs/historical",
        file_glob="**/*.jsonl"
    ) | read_jsonl()
    
    # Real-time logs from Kafka
    realtime_logs = kafka_consumer(["application-logs", "system-logs"])
    
    # Route to ClickHouse for analytics
    pipeline = dlt.pipeline(
        "log_analytics",
        destination="clickhouse",
        dataset_name="logs"
    )
    
    # Process historical data first
    historical_info = pipeline.run(
        historical_logs.with_name("historical_logs"),
        write_disposition="replace"
    )
    
    # Then append real-time data
    realtime_info = pipeline.run(
        realtime_logs.with_name("realtime_logs"),
        write_disposition="append"
    )
    
    return {"historical": historical_info, "realtime": realtime_info}
```

## 7. Troubleshooting Guide

### Common Issues and Solutions

```python
# Issue: ClickHouse float precision
# Solution: Use Decimal types for precise values
@dlt.resource(columns={"price": {"data_type": "decimal", "precision": 10, "scale": 2}})
def products():
    yield [{"id": 1, "price": "99.99"}]

# Issue: HDFS authentication
# Solution: Configure WebHDFS properly in secrets.toml
# [destination.filesystem.kwargs]
# kerberos = true
# token = "your_delegation_token"

# Issue: Kafka empty reads
# Solution: Ensure topics exist and have data, check consumer group
def debug_kafka_source():
    consumer = kafka_consumer(["test-topic"])
    # Add logging to see what's being consumed
    import logging
    logging.basicConfig(level=logging.DEBUG)
```

### Performance Optimization

```python
# Optimize for large datasets
def optimized_large_dataset_pipeline():
    pipeline = dlt.pipeline(
        "optimized",
        destination="clickhouse",
        dataset_name="large_data"
    )
    
    # Use parquet for better compression and performance
    source = your_large_source()
    info = pipeline.run(source, loader_file_format="parquet")
    return info

# Optimize batch sizes for real-time processing
def optimized_realtime_pipeline():
    pipeline = dlt.pipeline("realtime", destination="clickhouse", dataset_name="events")
    
    # Process smaller batches more frequently
    consumer = kafka_consumer(
        ["events"],
        batch_size=1000,  # Adjust based on message size and frequency
        batch_timeout=30  # Process batch every 30 seconds
    )
    
    return pipeline.run(consumer, write_disposition="append")
```

## 8. CLI Commands and Utilities

```bash
# Initialize new pipeline
dlt init rest_api postgres

# Show pipeline information
dlt pipeline my_pipeline show

# Run pipeline with specific configuration
dlt pipeline my_pipeline run

# Schema commands
dlt pipeline my_pipeline schema

# Drop pipeline datasets
dlt pipeline my_pipeline drop
```

## 9. Testing Patterns

```python
import dlt
import pytest
from unittest.mock import Mock

def test_pipeline_with_mock_data():
    """Test pipeline with mock data"""
    
    @dlt.resource
    def mock_data():
        yield [{"id": 1, "name": "test"}]
    
    pipeline = dlt.pipeline(
        "test_pipeline",
        destination="duckdb",  # Use DuckDB for testing
        dataset_name="test_data"
    )
    
    info = pipeline.run(mock_data())
    assert info.has_failed_jobs is False

def test_destination_routing():
    """Test smart destination routing"""
    profile = DataProfile(
        approx_rows=1000,
        approx_row_bytes=100,
        is_event_stream=False,
        update_rate="frequent_updates",
        nesting_depth=1,
        file_like=False,
        intended_use="oltp"
    )
    
    destination = choose_destination(profile)
    assert destination == "postgres"
```

This guide provides a comprehensive foundation for building data pipelines with dlt. The examples are production-ready and can be adapted for various data engineering scenarios in hackathons and real-world applications.