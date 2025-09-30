from typing import Dict, Any, List
import logging
from app.schemas.files import ColumnInfo

logger = logging.getLogger(__name__)


class DDLGenerator:
    """Generate DDL scripts for different destinations"""

    def __init__(self):
        self.type_mappings = {
            'postgresql': {
                'BIGINT': 'BIGINT',
                'DECIMAL': 'DECIMAL(18,2)',
                'VARCHAR': 'VARCHAR({max_length})',
                'TEXT': 'TEXT',
                'BOOLEAN': 'BOOLEAN',
                'TIMESTAMP': 'TIMESTAMP WITH TIME ZONE',
                'JSON': 'JSONB'
            },
            'clickhouse': {
                'BIGINT': 'Int64',
                'DECIMAL': 'Decimal64(2)',
                'VARCHAR': 'String',
                'TEXT': 'String',
                'BOOLEAN': 'UInt8',
                'TIMESTAMP': 'DateTime64',
                'JSON': 'String'
            }
        }

    def generate_ddl(self, schema_info: Dict[str, Any], destination_type: str, config: Dict[str, Any]) -> str:
        """Generate DDL script for the destination"""

        columns = schema_info.get('columns', [])
        if not columns:
            raise ValueError("No columns found in schema")

        # Normalize destination type
        destination_type = destination_type.lower()
        if destination_type in ('postgresql', 'postgres'):
            return self._generate_postgresql_ddl(columns, config)
        elif destination_type == 'clickhouse':
            return self._generate_clickhouse_ddl(columns, config)
        elif destination_type == 'hdfs':
            return self._generate_hdfs_ddl(columns, config)
        else:
            raise ValueError(f"Unsupported destination type: {destination_type}")

    def generate_etl_script(self, schema_info: Dict[str, Any], destination_type: str) -> str:
        """Generate ETL/DLT pipeline script"""

        columns = schema_info.get('columns', [])
        file_format = schema_info.get('file_format', 'csv')

        # Normalize destination type for dlt (uses 'postgres' not 'postgresql')
        dlt_destination = 'postgres' if destination_type.lower() in ('postgresql', 'postgres') else destination_type.lower()

        script_template = f"""
import dlt
from dlt.destinations import {dlt_destination}
import pandas as pd
from typing import Iterator, Dict, Any

@dlt.resource(
    name="processed_data",
    write_disposition="replace"  # or "append" for incremental
)
def load_data(file_path: str) -> Iterator[Dict[str, Any]]:
    \"\"\"Load and process data from {file_format.upper()} file\"\"\"

    # File processing logic based on format
    {self._get_file_processing_code(file_format, columns)}

    for chunk in chunks:
        for record in chunk.to_dict('records'):
            # Data transformation and validation
            processed_record = transform_record(record)
            yield processed_record

def transform_record(record: Dict[str, Any]) -> Dict[str, Any]:
    \"\"\"Transform and validate individual record\"\"\"

    transformed = {{}}

    # Column mappings and transformations
    {self._get_transformation_code(columns)}

    return transformed

# Pipeline configuration
if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="bigdata_processing",
        destination="{dlt_destination}",
        dataset_name="processed_data"
    )

    # Load data
    info = pipeline.run(
        load_data(file_path="{{{{ params.file_path }}}}"),
        table_name="{{{{ params.table_name }}}}"
    )

    print(f"Pipeline completed: {{info}}")
"""

        return script_template.strip()

    def _generate_postgresql_ddl(self, columns: List[ColumnInfo], config: Dict[str, Any]) -> str:
        """Generate PostgreSQL DDL"""

        table_name = config.get('table_name', 'processed_data')
        schema_name = config.get('schema_name', 'public')

        column_definitions = []

        for col in columns:
            sql_type = self._get_sql_type(col, 'postgresql')
            null_clause = "" if col.nullable else "NOT NULL"
            column_def = f"    {col.name} {sql_type} {null_clause}".strip()
            column_definitions.append(column_def)

        columns_join = ',\n    '.join(column_definitions)
        ddl = f"""-- PostgreSQL DDL Script
-- Generated for table: {schema_name}.{table_name}

DROP TABLE IF EXISTS {schema_name}.{table_name};

CREATE TABLE {schema_name}.{table_name} (
    id SERIAL PRIMARY KEY,
    {columns_join},
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX idx_{table_name}_created_at ON {schema_name}.{table_name}(created_at);

-- Add table comment
COMMENT ON TABLE {schema_name}.{table_name} IS 'Auto-generated table for BigData processing';
"""

        return ddl.strip()

    def _generate_clickhouse_ddl(self, columns: List[ColumnInfo], config: Dict[str, Any]) -> str:
        """Generate ClickHouse DDL"""

        table_name = config.get('table_name', 'processed_data')
        database = config.get('database', 'analytics')
        engine = config.get('engine', 'MergeTree')
        order_by = config.get('order_by', 'created_at')

        column_definitions = []

        for col in columns:
            sql_type = self._get_sql_type(col, 'clickhouse')
            # ClickHouse nullable syntax
            if col.nullable:
                sql_type = f"Nullable({sql_type})"

            column_def = f"    `{col.name}` {sql_type}"
            column_definitions.append(column_def)

        columns_join = ',\n    '.join(column_definitions)
        ddl = f"""-- ClickHouse DDL Script
-- Generated for table: {database}.{table_name}

DROP TABLE IF EXISTS {database}.{table_name};

CREATE TABLE {database}.{table_name} (
    `id` UInt64,
    {columns_join},
    `created_at` DateTime64 DEFAULT now64(),
    `updated_at` DateTime64 DEFAULT now64()
)
ENGINE = {engine}()
ORDER BY ({order_by})
SETTINGS index_granularity = 8192;

-- Add table comment
ALTER TABLE {database}.{table_name}
COMMENT 'Auto-generated table for BigData processing';
"""

        return ddl.strip()

    def _generate_hdfs_ddl(self, columns: List[ColumnInfo], config: Dict[str, Any]) -> str:
        """Generate HDFS/Parquet schema description"""

        path = config.get('path', '/bigdata/processed')
        file_format = config.get('file_format', 'parquet')

        column_info = []
        spark_columns = []
        for col in columns:
            parquet_type = self._get_parquet_type(col)
            column_info.append(f"  - {col.name}: {parquet_type}")
            spark_columns.append(f"{col.name} {self._get_spark_type(col)}")

        spark_columns_join = ',\n    '.join(spark_columns)
        column_info_join = '\n'.join(column_info)
        compression_value = config.get('compression', 'gzip')
        partition_by_value = config.get('partition_by', 'None')

        schema_description = f"""# HDFS Storage Schema
# Path: {path}
# Format: {file_format}

## Column Schema:
{column_info_join}

## dlt Configuration:
```python
import dlt

# HDFS destination configuration
destination_config = {{
    "path": "{path}",
    "file_format": "{file_format}",
    "compression": "{compression_value}",
    "partition_by": {partition_by_value}
}}
```

## Sample Spark DDL (for reference):
```sql
CREATE TABLE processed_data (
    id BIGINT,
    {spark_columns_join},
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING PARQUET
LOCATION '{path}'
```
"""

        return schema_description.strip()

    def _get_sql_type(self, column: ColumnInfo, destination_type: str) -> str:
        """Get SQL type for column based on destination"""

        base_type = column.data_type
        type_mapping = self.type_mappings.get(destination_type, {})

        sql_type = type_mapping.get(base_type, 'TEXT')

        # Handle VARCHAR with max length
        if base_type == 'VARCHAR' and column.max_length:
            max_length = min(column.max_length * 2, 65535)  # Buffer for safety
            sql_type = sql_type.format(max_length=max_length)
        elif '{max_length}' in sql_type:
            sql_type = sql_type.format(max_length=255)

        return sql_type

    def _get_parquet_type(self, column: ColumnInfo) -> str:
        """Get Parquet/Arrow type for column"""

        type_mapping = {
            'BIGINT': 'int64',
            'DECIMAL': 'decimal(18,2)',
            'VARCHAR': 'string',
            'TEXT': 'string',
            'BOOLEAN': 'bool',
            'TIMESTAMP': 'timestamp',
            'JSON': 'string'
        }

        return type_mapping.get(column.data_type, 'string')

    def _get_spark_type(self, column: ColumnInfo) -> str:
        """Get Spark SQL type for column"""

        type_mapping = {
            'BIGINT': 'BIGINT',
            'DECIMAL': 'DECIMAL(18,2)',
            'VARCHAR': 'STRING',
            'TEXT': 'STRING',
            'BOOLEAN': 'BOOLEAN',
            'TIMESTAMP': 'TIMESTAMP',
            'JSON': 'STRING'
        }

        return type_mapping.get(column.data_type, 'STRING')

    def _get_file_processing_code(self, file_format: str, columns: List[ColumnInfo]) -> str:
        """Generate file processing code based on format"""

        if file_format == 'csv':
            return """
    # Process CSV file in chunks for memory efficiency
    chunks = pd.read_csv(
        file_path,
        chunksize=10000,  # Process 10K rows at a time
        low_memory=False,
        encoding='utf-8'
    )"""

        elif file_format == 'json':
            return """
    # Process JSON file
    import json
    chunks = []
    with open(file_path, 'r') as f:
        if file_path.endswith('.jsonl'):
            # JSON Lines format
            data = [json.loads(line) for line in f]
        else:
            # Regular JSON
            data = json.load(f)

    # Convert to pandas chunks
    df = pd.DataFrame(data)
    chunk_size = 10000
    chunks = [df[i:i+chunk_size] for i in range(0, len(df), chunk_size)]"""

        elif file_format == 'xml':
            return """
    # Process XML file
    import xml.etree.ElementTree as ET
    import xmltodict

    with open(file_path, 'r') as f:
        xml_data = xmltodict.parse(f.read())

    # Extract records and convert to DataFrame
    # This assumes your XML has repeating record elements
    records = []  # Extract your records here based on XML structure
    df = pd.DataFrame(records)
    chunk_size = 10000
    chunks = [df[i:i+chunk_size] for i in range(0, len(df), chunk_size)]"""

        return "    # Generic file processing code here"

    def _get_transformation_code(self, columns: List[ColumnInfo]) -> str:
        """Generate transformation code for columns"""

        transformations = []

        for col in columns:
            if col.data_type == 'TIMESTAMP':
                transformations.append(f"""
    # Transform {col.name} to timestamp
    if '{col.name}' in record:
        try:
            transformed['{col.name}'] = pd.to_datetime(record['{col.name}']).isoformat()
        except:
            transformed['{col.name}'] = None""")

            elif col.data_type == 'BIGINT':
                transformations.append(f"""
    # Transform {col.name} to integer
    if '{col.name}' in record:
        try:
            transformed['{col.name}'] = int(record['{col.name}']) if record['{col.name}'] is not None else None
        except:
            transformed['{col.name}'] = None""")

            elif col.data_type == 'DECIMAL':
                transformations.append(f"""
    # Transform {col.name} to decimal
    if '{col.name}' in record:
        try:
            transformed['{col.name}'] = float(record['{col.name}']) if record['{col.name}'] is not None else None
        except:
            transformed['{col.name}'] = None""")

            else:
                transformations.append(f"""
    # Transform {col.name}
    transformed['{col.name}'] = record.get('{col.name}')""")

        return '\n    '.join(transformations) if transformations else "    pass"