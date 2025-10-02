from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from typing import List
import aiofiles
import os
import uuid
import json
from datetime import datetime
import logging

from app.database import get_db, engine
from app.models.jobs import ProcessingJob, ProcessingProgress
from app.models.files import FileMetadata
from app.schemas.jobs import JobResponse, JobStatus, AirflowCallback
from app.schemas.files import FileUploadResponse
from app.core.file_processor import FileProcessor
from app.core.ddl_generator import DDLGenerator
from app.core.airflow_client import AirflowClient
from app.config import settings

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/upload", response_model=FileUploadResponse)
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    destination_type: str = Form(...),
    destination_config: str = Form(...),
    db: AsyncSession = Depends(get_db)
):
    """Upload file and create processing job"""

    # Validate file size
    if file.size and file.size > settings.MAX_FILE_SIZE:
        raise HTTPException(status_code=413, detail="File too large")

    # Validate file format
    file_format = file.filename.split('.')[-1].lower()
    if file_format not in settings.ALLOWED_FILE_FORMATS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported format: {file_format}. Allowed: {settings.ALLOWED_FILE_FORMATS}"
        )

    # Generate job ID and file path
    job_id = str(uuid.uuid4())
    # Sanitize filename to avoid path issues - use only base filename
    # Handle both forward and backward slashes, and extract just the filename
    safe_filename = file.filename.replace('\\', '/').split('/')[-1]
    # Also remove any remaining problematic characters
    safe_filename = safe_filename.replace(' ', '_').replace('(', '').replace(')', '')
    file_path = os.path.join(settings.DATA_INPUT_PATH, f"{job_id}_{safe_filename}")

    # Save file
    try:
        async with aiofiles.open(file_path, 'wb') as f:
            content = await file.read()
            await f.write(content)

        actual_file_size = os.path.getsize(file_path)
        logger.info(f"File saved: {file_path}, size: {actual_file_size}")

    except Exception as e:
        logger.error(f"Error saving file: {e}")
        raise HTTPException(status_code=500, detail="Failed to save file")

    # Parse destination config
    try:
        destination_config_dict = json.loads(destination_config)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid destination config JSON")

    # Map destination_type to destination value for database constraint
    destination_mapping = {
        'postgresql': 'postgres',
        'postgres': 'postgres',
        'clickhouse': 'clickhouse',
        'hdfs': 'hdfs',
        's3': 's3'
    }

    destination = destination_mapping.get(destination_type.lower(), 'postgres')

    # Create processing job record
    job = ProcessingJob(
        job_id=job_id,
        filename=file.filename,
        file_size=actual_file_size,
        file_type=file_format,
        destination=destination,
        destination_config=destination_config_dict,
        status='pending'
    )

    db.add(job)
    await db.commit()
    await db.refresh(job)

    # Start background processing using asyncio.create_task (runs in main event loop)
    logger.info(f"Starting background processing for job {job_id}")
    import asyncio
    asyncio.create_task(process_file_background(job_id, file_path, file_format, destination, destination_config_dict))
    logger.info(f"Background task started for job {job_id}")

    return FileUploadResponse(
        job_id=job_id,
        filename=file.filename,
        file_size=actual_file_size,
        file_format=file_format,
        message="File uploaded successfully, processing started"
    )


async def process_file_background(job_id: str, file_path: str, file_format: str, destination: str, destination_config: dict):
    """Background task for file processing"""
    from app.database import async_session_maker

    logger.info(f"Starting background processing for job {job_id}, file: {file_path}")

    async with async_session_maker() as db:
        try:
            logger.info(f"Starting database transaction for job {job_id}")
            # Update job status
            await db.execute(
                update(ProcessingJob)
                .where(ProcessingJob.job_id == job_id)
                .values(status='processing', started_at=datetime.utcnow())
            )
            await db.commit()
            logger.info(f"Job status updated to processing for {job_id}")

            # Publish File Analysis start via Redis
            import redis.asyncio as aioredis
            import json
            import os
            redis_client = aioredis.from_url(os.getenv('REDIS_URL', 'redis://redis:6379/0'), decode_responses=True)
            try:
                progress_data = {
                    'job_id': job_id,
                    'type': 'progress_update',
                    'data': {
                        'status': 'processing',
                        'stage': 'File Analysis',
                        'progress_percent': 10.0,
                        'records_processed': 0,
                        'total_records': 0,
                        'message': 'Analyzing file structure...'
                    },
                    'timestamp': datetime.utcnow().isoformat()
                }
                await redis_client.publish(f'job_progress:{job_id}', json.dumps(progress_data))
                logger.info(f"Published File Analysis start to Redis for {job_id}")
            finally:
                await redis_client.close()

            # Analyze file
            file_processor = FileProcessor()
            schema_info = await file_processor.analyze_file(file_path, file_format)

            # Publish DDL Generation start via Redis
            redis_client = aioredis.from_url(os.getenv('REDIS_URL', 'redis://redis:6379/0'), decode_responses=True)
            try:
                progress_data = {
                    'job_id': job_id,
                    'type': 'progress_update',
                    'data': {
                        'status': 'processing',
                        'stage': 'DDL Generation',
                        'progress_percent': 30.0,
                        'records_processed': 0,
                        'total_records': 0,
                        'message': 'Generating DDL and ETL scripts...'
                    },
                    'timestamp': datetime.utcnow().isoformat()
                }
                await redis_client.publish(f'job_progress:{job_id}', json.dumps(progress_data))
                logger.info(f"Published DDL Generation start to Redis for {job_id}")
            finally:
                await redis_client.close()

            # Generate DDL and ETL scripts
            logger.info(f"Generating DDL and ETL scripts for job {job_id}")
            ddl_generator = DDLGenerator()
            ddl_script = ddl_generator.generate_ddl(schema_info, destination, destination_config)
            etl_script = ddl_generator.generate_etl_script(schema_info, destination)
            logger.info(f"Generated DDL ({len(ddl_script)} chars) and ETL ({len(etl_script)} chars) scripts")

            # Save DDL and ETL scripts to job
            try:
                result = await db.execute(
                    update(ProcessingJob)
                    .where(ProcessingJob.job_id == job_id)
                    .values(ddl_script=ddl_script, etl_script=etl_script)
                )
                await db.commit()
                logger.info(f"Saved DDL and ETL scripts for job {job_id} (rows affected: {result.rowcount})")

                # Publish DDL/ETL available event via Redis
                redis_client = aioredis.from_url(os.getenv('REDIS_URL', 'redis://redis:6379/0'), decode_responses=True)
                try:
                    progress_data = {
                        'job_id': job_id,
                        'type': 'progress_update',
                        'data': {
                            'status': 'processing',
                            'stage': 'DDL Generated',
                            'progress_percent': 40.0,
                            'records_processed': 0,
                            'total_records': 0,
                            'message': 'DDL and ETL scripts generated',
                            'ddl_script': ddl_script,
                            'etl_script': etl_script
                        },
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    await redis_client.publish(f'job_progress:{job_id}', json.dumps(progress_data))
                    logger.info(f"Published DDL Generated event to Redis for {job_id}")
                finally:
                    await redis_client.close()
            except Exception as save_error:
                logger.error(f"Failed to save DDL/ETL scripts: {save_error}")
                # Continue processing even if script save fails

            # Analysis completed - job remains in processing status
            logger.info(f"File analysis completed for job {job_id}, continuing to data processing")

            # Save file metadata with proper JSON serialization
            logger.info(f"Storing file metadata for job {job_id}")

            # Convert ColumnInfo objects to dictionaries for JSON serialization
            # Optimize for large files: limit columns and sample data to prevent database errors
            file_size = schema_info.get('file_size', 0)
            is_large_file = file_size > 50 * 1024 * 1024  # Files > 50MB
            max_columns = 50 if is_large_file else 200  # Limit columns for large files

            columns_data = []
            for i, col in enumerate(schema_info.get('columns', [])):
                if i >= max_columns:
                    break  # Stop at max columns limit

                if hasattr(col, 'model_dump'):
                    col_dict = col.model_dump()
                elif hasattr(col, 'dict'):
                    col_dict = col.dict()
                else:
                    # Manual conversion for safety
                    col_dict = {
                        'name': col.name,
                        'data_type': col.data_type,
                        'nullable': col.nullable,
                        'max_length': col.max_length,
                        'sample_values': col.sample_values[:3] if hasattr(col, 'sample_values') else []
                    }

                # Limit sample values for large files and clean NaN values
                if 'sample_values' in col_dict:
                    import math
                    clean_samples = []
                    for sample in col_dict['sample_values']:
                        if isinstance(sample, float) and (math.isnan(sample) or math.isinf(sample)):
                            clean_samples.append(None)
                        else:
                            clean_samples.append(sample)

                    if is_large_file:
                        col_dict['sample_values'] = clean_samples[:2]  # Only 2 samples for large files
                    else:
                        col_dict['sample_values'] = clean_samples

                columns_data.append(col_dict)

            # Add metadata about truncation
            total_columns = len(schema_info.get('columns', []))
            if total_columns > max_columns:
                columns_data.append({
                    'name': f'... and {total_columns - max_columns} more columns',
                    'data_type': 'INFO',
                    'nullable': True,
                    'max_length': None,
                    'sample_values': []
                })

            # Create a clean schema_info dict without ColumnInfo objects
            # Universal function to clean all NaN/inf values from any data structure
            def deep_clean_nan(obj):
                """Recursively clean NaN, inf, -inf from any data structure"""
                import math
                import pandas as pd

                if isinstance(obj, dict):
                    return {k: deep_clean_nan(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [deep_clean_nan(item) for item in obj]
                elif pd.isna(obj):
                    return None
                elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
                    return None
                elif str(obj).lower() in ['nan', 'inf', '-inf']:
                    return None
                else:
                    return obj

            # Clean sample data - apply to raw data first
            sample_data_raw = schema_info.get('sample_data', [])[:3] if is_large_file else schema_info.get('sample_data', [])[:5]
            sample_data = deep_clean_nan(sample_data_raw)

            schema_info_clean = {
                'file_path': schema_info.get('file_path'),
                'file_size': file_size,
                'encoding': schema_info.get('encoding'),
                'file_format': schema_info.get('file_format'),
                'columns': columns_data,  # Optimized for large files
                'sample_data': sample_data,  # Limited for large files
                'total_rows': schema_info.get('total_rows'),
                'total_columns': total_columns,  # Add total count
                'analysis_timestamp': str(schema_info.get('analysis_timestamp', '')),
                'mime_type': schema_info.get('mime_type'),
                'csv_delimiter': schema_info.get('csv_delimiter'),
                'csv_quotechar': schema_info.get('csv_quotechar'),
                'is_large_file': is_large_file  # Flag for UI
            }

            file_metadata = FileMetadata(
                job_id=job_id,
                original_filename=os.path.basename(file_path),
                file_path=file_path,
                file_size=os.path.getsize(file_path),
                mime_type=schema_info.get('mime_type'),
                encoding=schema_info.get('encoding'),
                delimiter=schema_info.get('csv_delimiter'),
                quote_char=schema_info.get('csv_quotechar'),
                header_row=True,
                columns_detected=deep_clean_nan(columns_data),
                sample_data=deep_clean_nan(sample_data),
                data_quality_score=None,
                schema_validation_errors=[]
            )

            db.add(file_metadata)
            await db.commit()
            logger.info(f"File metadata stored successfully for job {job_id}")

            # Try to trigger Airflow DAG (non-blocking)
            try:
                logger.info(f"Attempting to trigger Airflow DAG for job {job_id}")
                airflow_client = AirflowClient()
                dag_run_id = await airflow_client.trigger_dag(str(job_id), {
                    'job_id': str(job_id),
                    'file_path': file_path,
                    'file_format': file_format,
                    'destination': destination,
                    'destination_config': destination_config
                })

                # Airflow DAG triggered, job already in processing status
                await db.commit()
                logger.info(f"Airflow DAG triggered successfully for job {job_id}, dag_run_id: {dag_run_id}")

            except Exception as airflow_error:
                logger.warning(f"Airflow unavailable for job {job_id}: {airflow_error}")
                logger.info(f"Starting fallback ETL processing for job {job_id}")

                # Process data directly without Airflow
                try:
                    records_processed = await _process_data_directly(
                        job_id, file_path, file_format, destination, destination_config, schema_info, db
                    )

                    # Update job as completed
                    await db.execute(
                        update(ProcessingJob)
                        .where(ProcessingJob.job_id == job_id)
                        .values(
                            status='completed',
                            completed_at=datetime.utcnow(),
                            error_message=f"Completed with fallback ETL (Airflow unavailable), processed {records_processed} records"
                        )
                    )
                    await db.commit()
                    logger.info(f"Job {job_id} completed successfully with {records_processed} records processed")

                    # Publish job completed event via Redis
                    redis_client = aioredis.from_url(os.getenv('REDIS_URL', 'redis://redis:6379/0'), decode_responses=True)
                    try:
                        progress_data = {
                            'job_id': job_id,
                            'type': 'job_completed',
                            'data': {
                                'status': 'completed',
                                'stage': 'Completed',
                                'progress_percent': 100.0,
                                'records_processed': records_processed,
                                'total_records': records_processed,
                                'message': f'Job completed successfully: {records_processed} records processed'
                            },
                            'timestamp': datetime.utcnow().isoformat()
                        }
                        await redis_client.publish(f'job_progress:{job_id}', json.dumps(progress_data))
                        logger.info(f"Published job completed event to Redis for {job_id}")
                    finally:
                        await redis_client.close()

                except Exception as etl_error:
                    logger.error(f"Fallback ETL failed for job {job_id}: {etl_error}")
                    await db.execute(
                        update(ProcessingJob)
                        .where(ProcessingJob.job_id == job_id)
                        .values(
                            status='failed',
                            completed_at=datetime.utcnow(),
                            error_message=f"ETL processing failed: {str(etl_error)}"
                        )
                    )
                    await db.commit()
                    raise etl_error

            logger.info(f"File processing pipeline completed for job {job_id}")

        except Exception as e:
            logger.error(f"Critical error in file processing for job {job_id}: {e}")

            # Update job with error - only for critical errors, not Airflow issues
            try:
                await db.execute(
                    update(ProcessingJob)
                    .where(ProcessingJob.job_id == job_id)
                    .values(status='failed', error_message=str(e), completed_at=datetime.utcnow())
                )
                await db.commit()
            except Exception as db_error:
                logger.error(f"Failed to update job status: {db_error}")


def _pandas_processing_sync(job_id: str, file_path: str, file_format: str, table_name: str, schema_info: dict, database_url: str) -> dict:
    """Pure synchronous function for pandas processing - runs in thread pool to avoid event loop conflicts"""
    import pandas as pd
    from sqlalchemy import create_engine, text
    from datetime import datetime
    import os
    import xml.etree.ElementTree as ET
    import psycopg2
    from psycopg2.extras import execute_values
    from urllib.parse import urlparse
    import redis
    import json

    # Disable pandas/numpy multithreading to prevent segfaults
    os.environ['OMP_NUM_THREADS'] = '1'
    os.environ['OPENBLAS_NUM_THREADS'] = '1'
    os.environ['MKL_NUM_THREADS'] = '1'
    os.environ['VECLIB_MAXIMUM_THREADS'] = '1'
    os.environ['NUMEXPR_NUM_THREADS'] = '1'

    # Create synchronous database engine for DDL and progress tracking
    sync_engine = create_engine(database_url)

    # Create synchronous Redis client for real-time progress updates
    redis_url = os.getenv('REDIS_URL', 'redis://redis:6379/0')
    redis_client = redis.from_url(redis_url, decode_responses=True)

    # Parse database URL for direct psycopg2 connection (faster, no SEGFAULT)
    parsed = urlparse(database_url)
    pg_conn = psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port,
        user=parsed.username,
        password=parsed.password,
        database=parsed.path[1:]  # Remove leading /
    )

    # Get file parameters
    delimiter = schema_info.get('csv_delimiter', ',')
    encoding = schema_info.get('encoding', 'utf-8')
    chunk_size = 10000  # Larger chunks for better performance (was 5000)

    logger.info(f"[SYNC] Processing {file_format.upper()} file: {file_path}, encoding '{encoding}'")

    # Read first sample to create table schema
    if file_format.lower() == 'csv':
        first_chunk = pd.read_csv(file_path, nrows=1000, delimiter=delimiter, encoding=encoding, dtype=str, low_memory=False)
    elif file_format.lower() == 'json':
        try:
            first_chunk = pd.read_json(file_path, lines=True, nrows=1000, encoding=encoding, dtype=str)
        except:
            logger.info("[SYNC] JSON Lines format failed, trying array format")
            df = pd.read_json(file_path, encoding=encoding, dtype=str)
            first_chunk = df.head(1000)
    elif file_format.lower() == 'xml':
        tree = ET.parse(file_path)
        root = tree.getroot()
        records = []
        for i, elem in enumerate(root):
            if i >= 1000:
                break
            record = {child.tag: child.text for child in elem}
            records.append(record)
        first_chunk = pd.DataFrame(records)
        first_chunk = first_chunk.astype(str)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

    if first_chunk.empty:
        logger.warning(f"[SYNC] File is empty: {file_path}")
        return {'total_processed': 0, 'skipped_chunks': 0, 'estimated_total': 0}

    # Clean column names
    first_chunk.columns = [col.strip().replace(' ', '_').replace('-', '_').replace('.', '_')
                          for col in first_chunk.columns]

    # Create table with TEXT columns
    columns_ddl = ', '.join([f'"{col}" TEXT' for col in first_chunk.columns])
    create_table_sql = f"DROP TABLE IF EXISTS {table_name}; CREATE TABLE {table_name} ({columns_ddl});"

    logger.info(f"[SYNC] Creating table with {len(first_chunk.columns)} TEXT columns")

    with sync_engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()

    logger.info(f"[SYNC] Created table: {table_name}")

    # Count actual records
    actual_file_lines = 0
    is_json_array = False

    try:
        if file_format.lower() == 'csv':
            with open(file_path, 'r', encoding=encoding) as f:
                actual_file_lines = sum(1 for _ in f) - 1
        elif file_format.lower() == 'json':
            with open(file_path, 'r', encoding=encoding) as f:
                first_char = f.read(1).strip()
                if first_char == '[':
                    is_json_array = True
                    # IMPORTANT: ijson.items() can cause SEGFAULT with invalid Unicode
                    # Using safe json.load() with error handling instead
                    import json
                    with open(file_path, 'r', encoding=encoding, errors='replace') as jf:
                        data = json.load(jf)
                        if data is None:
                            logger.error("[SYNC] JSON load returned None")
                            actual_file_lines = 0
                        else:
                            actual_file_lines = len(data) if isinstance(data, list) else 1
                    logger.info(f"[SYNC] JSON array format: {actual_file_lines} records")
                else:
                    with open(file_path, 'r', encoding=encoding) as f:
                        actual_file_lines = sum(1 for _ in f)
                    logger.info(f"[SYNC] JSON Lines format: {actual_file_lines} records")
        elif file_format.lower() == 'xml':
            tree = ET.parse(file_path)
            root = tree.getroot()
            actual_file_lines = len(root)
        logger.info(f"[SYNC] Actual record count: {actual_file_lines}")
    except Exception as count_error:
        logger.warning(f"[SYNC] Could not count records: {count_error}")

    estimated_total = actual_file_lines if actual_file_lines > 0 else schema_info.get('total_rows', 0)

    # Create chunk iterator
    if file_format.lower() == 'csv':
        chunk_iter = pd.read_csv(
            file_path,
            chunksize=chunk_size,
            delimiter=delimiter,
            encoding=encoding,
            dtype=str,
            on_bad_lines='warn',
            engine='python',
            quoting=3,
            escapechar=None
        )
    elif file_format.lower() == 'json':
        if is_json_array:
            # IMPORTANT: ijson.items() causes SEGFAULT with invalid Unicode
            # Using safe json.load() with chunking instead
            import json
            def json_array_chunk_generator(file_path, chunk_size):
                with open(file_path, 'r', encoding=encoding, errors='replace') as jf:
                    data = json.load(jf)
                    if data is None:
                        logger.error(f"[SYNC] JSON file is empty or corrupted")
                        raise ValueError("JSON file is empty or corrupted")
                    if not isinstance(data, list):
                        data = [data]
                    for i in range(0, len(data), chunk_size):
                        chunk_data = data[i:i+chunk_size]
                        yield pd.DataFrame(chunk_data).astype(str)
            chunk_iter = json_array_chunk_generator(file_path, chunk_size)
        else:
            chunk_iter = pd.read_json(
                file_path,
                lines=True,
                chunksize=chunk_size,
                encoding=encoding,
                dtype=str
            )
    elif file_format.lower() == 'xml':
        tree = ET.parse(file_path)
        root = tree.getroot()

        def xml_chunk_generator(xml_root, chunk_size):
            records = []
            for elem in xml_root:
                record = {child.tag: child.text for child in elem}
                records.append(record)
                if len(records) >= chunk_size:
                    df = pd.DataFrame(records)
                    df = df.astype(str)
                    yield df
                    records = []
            if records:
                df = pd.DataFrame(records)
                df = df.astype(str)
                yield df

        chunk_iter = xml_chunk_generator(root, chunk_size)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

    # Process chunks
    total_processed = 0
    skipped_chunks = 0
    last_chunk_num = 0

    # Get column names for insert statement
    columns_list = None

    for chunk_num, chunk in enumerate(chunk_iter):
        try:
            # Clean column names
            chunk.columns = [col.strip().replace(' ', '_').replace('-', '_').replace('.', '_')
                           for col in chunk.columns]

            # Store column list on first chunk
            if columns_list is None:
                columns_list = list(chunk.columns)

            # Replace NaN/None with None explicitly for proper NULL handling
            chunk = chunk.where(chunk.notnull(), None)

            # COPY TEXT format is safest - escapes all special characters automatically
            from io import StringIO

            # Build TEXT format data (tab-separated, \N for NULL, escapes handled by psycopg2)
            text_buffer = StringIO()

            for row in chunk.values:
                row_values = []
                for v in row:
                    if v is None or (isinstance(v, float) and pd.isna(v)):
                        row_values.append('\\N')
                    else:
                        # Escape special characters for PostgreSQL TEXT format
                        str_val = str(v).replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
                        row_values.append(str_val)
                text_buffer.write('\t'.join(row_values) + '\n')

            text_buffer.seek(0)

            # Use COPY TEXT format (most reliable for problematic data)
            columns_str = ', '.join([f'"{col}"' for col in columns_list])
            # Escape % as %% for copy_expert (prevents "not all arguments converted" error)
            copy_sql = f"COPY {table_name} ({columns_str}) FROM STDIN WITH (FORMAT TEXT)".replace('%', '%%')

            with pg_conn.cursor() as cursor:
                cursor.copy_expert(copy_sql, text_buffer)

            # Commit every 5 chunks for better performance (was every chunk)
            if chunk_num % 5 == 0:
                pg_conn.commit()

            total_processed += len(chunk)
            last_chunk_num = chunk_num

            # Log and publish progress every 5 chunks (reduced overhead)
            if chunk_num % 5 == 0:
                progress_percent = round((total_processed / estimated_total * 100), 2) if estimated_total > 0 else 0
                logger.info(f"[SYNC] Chunk {chunk_num}: {total_processed}/{estimated_total} ({progress_percent}%)")

                # Save progress to database
                try:
                    with sync_engine.connect() as conn:
                        conn.execute(text("""
                            INSERT INTO processing_progress
                            (job_id, stage, stage_order, progress_percent, records_processed, total_records, message, timestamp)
                            VALUES (:job_id, :stage, :stage_order, :progress_percent, :records_processed, :total_records, :message, :timestamp)
                        """), {
                            'job_id': job_id,
                            'stage': 'Data Loading',
                            'stage_order': 4,
                            'progress_percent': progress_percent,
                            'records_processed': total_processed,
                            'total_records': estimated_total,
                            'message': f'Loading data chunk {chunk_num}',
                            'timestamp': datetime.utcnow()
                        })
                        conn.commit()
                except Exception as progress_error:
                    logger.warning(f"[SYNC] Progress save failed: {progress_error}")

                # Publish real-time progress to Redis
                try:
                    progress_data = {
                        'job_id': job_id,
                        'type': 'progress_update',
                        'data': {
                            'status': 'processing',
                            'stage': 'Data Loading',
                            'progress_percent': progress_percent,
                            'records_processed': total_processed,
                            'total_records': estimated_total,
                            'message': f'Processing chunk {chunk_num}...'
                        },
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    redis_client.publish(f'job_progress:{job_id}', json.dumps(progress_data))
                    logger.info(f"[SYNC] Published progress to Redis: {progress_percent}%")
                except Exception as redis_error:
                    logger.warning(f"[SYNC] Redis publish failed: {redis_error}")

        except Exception as chunk_error:
            skipped_chunks += 1
            logger.error(f"[SYNC] Chunk {chunk_num} failed: {chunk_error}")
            continue

    # Final commit for any remaining uncommitted chunks
    pg_conn.commit()

    # Always publish final progress (ensure 100% is shown)
    if total_processed > 0:
        final_progress_percent = 100.0 if total_processed >= estimated_total else round((total_processed / estimated_total * 100), 2)
        logger.info(f"[SYNC] Final progress: {total_processed}/{estimated_total} ({final_progress_percent}%)")

        # Save final progress to database
        try:
            with sync_engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO processing_progress
                    (job_id, stage, stage_order, progress_percent, records_processed, total_records, message, timestamp)
                    VALUES (:job_id, :stage, :stage_order, :progress_percent, :records_processed, :total_records, :message, :timestamp)
                """), {
                    'job_id': job_id,
                    'stage': 'Data Loading',
                    'stage_order': 4,
                    'progress_percent': final_progress_percent,
                    'records_processed': total_processed,
                    'total_records': estimated_total,
                    'message': f'Completed: {total_processed} records loaded',
                    'timestamp': datetime.utcnow()
                })
                conn.commit()
        except Exception as progress_error:
            logger.warning(f"[SYNC] Final progress save failed: {progress_error}")

        # Publish final progress to Redis
        try:
            final_data = {
                'job_id': job_id,
                'type': 'progress_update',
                'data': {
                    'status': 'processing',
                    'stage': 'Data Loading',
                    'progress_percent': final_progress_percent,
                    'records_processed': total_processed,
                    'total_records': estimated_total,
                    'message': f'Completed: {total_processed} records loaded'
                },
                'timestamp': datetime.utcnow().isoformat()
            }
            redis_client.publish(f'job_progress:{job_id}', json.dumps(final_data))
            logger.info(f"[SYNC] Published final progress to Redis: {final_progress_percent}%")
        except Exception as redis_error:
            logger.warning(f"[SYNC] Final Redis publish failed: {redis_error}")

    logger.info(f"[SYNC] Processing complete: {total_processed} records, {skipped_chunks} chunks skipped")

    # Close connections
    pg_conn.close()
    redis_client.close()

    return {
        'total_processed': total_processed,
        'skipped_chunks': skipped_chunks,
        'estimated_total': estimated_total
    }


async def _process_data_directly(job_id, file_path: str, file_format: str, destination: str, destination_config: dict, schema_info: dict, db):
    """Direct data processing when Airflow is unavailable"""
    import asyncio
    import os

    logger.info(f"Starting direct data processing for job {job_id} with format: {file_format}")

    # Validate supported formats
    supported_formats = ['csv', 'json', 'xml']
    if file_format.lower() not in supported_formats:
        raise ValueError(f"Direct processing only supports {supported_formats}, got: {file_format}")

    # Generate table name
    table_name = f"processed_data_{str(job_id).replace('-', '_')}"

    try:
        # Get database URL for sync engine
        database_url = os.getenv('DATABASE_URL', '').replace('postgresql+asyncpg://', 'postgresql://')

        # Run entire pandas processing in thread pool to avoid async/sync conflicts
        logger.info(f"Running pandas processing in thread pool for job {job_id}")
        result = await asyncio.to_thread(
            _pandas_processing_sync,
            job_id,
            file_path,
            file_format,
            table_name,
            schema_info,
            database_url
        )

        total_processed = result['total_processed']
        skipped_chunks = result['skipped_chunks']
        estimated_total = result['estimated_total']

        logger.info(f"Pandas processing completed: {total_processed}/{estimated_total} records, {skipped_chunks} chunks skipped")

        if skipped_chunks > 0:
            logger.error(f"WARNING: {skipped_chunks} chunks failed during processing")

        if estimated_total > 0 and total_processed < estimated_total:
            data_loss = estimated_total - total_processed
            loss_percent = round((data_loss / estimated_total * 100), 2)
            logger.warning(f"DATA LOSS: {data_loss} records missing ({loss_percent}%)")
        elif estimated_total > 0:
            logger.info(f"SUCCESS: All {estimated_total} records processed")

        return total_processed

    except Exception as e:
        logger.error(f"Error in direct data processing for job {job_id}: {e}")
        raise e


@router.post("/{job_id}/process")
async def process_job_manually(job_id: str, db: AsyncSession = Depends(get_db)):
    """Manually trigger job processing"""

    # Get job details
    result = await db.execute(select(ProcessingJob).where(ProcessingJob.job_id == job_id))
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Start processing in background using asyncio.create_task
    import asyncio
    asyncio.create_task(
        process_file_background(
            job_id,
            f"/app/data/input/{job_id}_{job.filename}",
            job.file_type,
            job.destination,
            job.destination_config
        )
    )

    return {"message": "Processing started", "job_id": job_id}


@router.get("/", response_model=List[JobResponse])
async def get_jobs(db: AsyncSession = Depends(get_db)):
    """Get all processing jobs"""

    result = await db.execute(
        select(ProcessingJob).order_by(ProcessingJob.created_at.desc())
    )
    jobs = result.scalars().all()
    return [JobResponse.model_validate(job) for job in jobs]


@router.get("/{job_id}")
async def get_job(job_id: str, db: AsyncSession = Depends(get_db)):
    """Get specific processing job"""

    result = await db.execute(
        select(ProcessingJob).where(ProcessingJob.job_id == job_id)
    )
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Get latest progress for this job
    progress_result = await db.execute(
        select(ProcessingProgress)
        .where(ProcessingProgress.job_id == job_id)
        .order_by(ProcessingProgress.timestamp.desc())
        .limit(1)
    )
    latest_progress = progress_result.scalar_one_or_none()

    # Create response using serializer
    response = JobResponse.model_validate(job)
    response_dict = response.serialize_model()

    # Override progress fields with actual progress data
    if latest_progress:
        logger.info(f"Found progress for job {job_id}: {latest_progress.records_processed}/{latest_progress.total_records}")
        response_dict['records_processed'] = latest_progress.records_processed or 0
        response_dict['records_total'] = latest_progress.total_records or 0
        response_dict['progress_percentage'] = float(latest_progress.progress_percent or 0)
    else:
        logger.warning(f"No progress found for job {job_id}")

    return response_dict


@router.get("/{job_id}/status", response_model=JobStatus)
async def get_job_status(job_id: str, db: AsyncSession = Depends(get_db)):
    """Get job processing status"""

    result = await db.execute(
        select(ProcessingJob).where(ProcessingJob.job_id == job_id)
    )
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Get latest progress
    progress_result = await db.execute(
        select(ProcessingProgress)
        .where(ProcessingProgress.job_id == job_id)
        .order_by(ProcessingProgress.timestamp.desc())
        .limit(1)
    )
    latest_progress = progress_result.scalar_one_or_none()

    progress_percentage = 0.0
    current_stage = None
    records_processed = 0
    records_total = 0

    if latest_progress:
        progress_percentage = float(latest_progress.progress_percent or 0)
        current_stage = latest_progress.stage
        records_processed = latest_progress.records_processed or 0
        records_total = latest_progress.total_records or 0

    return JobStatus(
        job_id=str(job.job_id),
        status=job.status,
        records_processed=records_processed,
        records_total=records_total,
        progress_percentage=progress_percentage,
        current_stage=current_stage,
        error_message=job.error_message
    )


@router.delete("/{job_id}")
async def cancel_job(job_id: str, db: AsyncSession = Depends(get_db)):
    """Cancel processing job"""

    # Get job
    result = await db.execute(
        select(ProcessingJob).where(ProcessingJob.job_id == job_id)
    )
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Update job status
    await db.execute(
        update(ProcessingJob)
        .where(ProcessingJob.job_id == job_id)
        .values(status='cancelled', completed_at=datetime.utcnow())
    )
    await db.commit()

    return {"message": "Job cancelled successfully"}


@router.post("/callback/airflow")
async def airflow_callback(callback_data: AirflowCallback, db: AsyncSession = Depends(get_db)):
    """Receive callbacks from Airflow DAGs"""

    # Update job status
    await db.execute(
        update(ProcessingJob)
        .where(ProcessingJob.job_id == callback_data.job_id)
        .values(
            status=callback_data.status,
            error_message=callback_data.error_message
        )
    )

    # Add progress record
    progress = ProcessingProgress(
        job_id=callback_data.job_id,
        stage=callback_data.stage,
        stage_order=0,
        progress_percent=callback_data.progress_percent or 0,
        records_processed=callback_data.records_processed or 0,
        total_records=callback_data.total_records or 0,
        message=callback_data.message
    )

    db.add(progress)
    await db.commit()

    return {"status": "callback received"}


@router.get("/{job_id}/ddl")
async def get_job_ddl(job_id: str, db: AsyncSession = Depends(get_db)):
    """Get generated DDL script for job"""

    result = await db.execute(
        select(ProcessingJob)
        .where(ProcessingJob.job_id == job_id)
    )
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "job_id": job_id,
        "ddl_script": None,
        "etl_script": None,
        "message": "DDL/ETL scripts not implemented yet"
    }