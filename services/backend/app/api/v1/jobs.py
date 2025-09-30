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

    # Start background processing
    logger.info(f"Adding background task for job {job_id}")
    background_tasks.add_task(process_file_background_wrapper, job_id, file_path, file_format, destination, destination_config_dict)
    logger.info(f"Background task added for job {job_id}")

    return FileUploadResponse(
        job_id=job_id,
        filename=file.filename,
        file_size=actual_file_size,
        file_format=file_format,
        message="File uploaded successfully, processing started"
    )


def process_file_background_wrapper(job_id: str, file_path: str, file_format: str, destination: str, destination_config: dict):
    """Synchronous wrapper for background task"""
    import asyncio
    import traceback

    logger.info(f"Background wrapper started for job {job_id}")

    try:
        # Create new event loop for background task
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Run the async function
        loop.run_until_complete(process_file_background(job_id, file_path, file_format, destination, destination_config))
        logger.info(f"Background processing completed for job {job_id}")
    except Exception as e:
        logger.error(f"Background processing failed for job {job_id}: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
    finally:
        loop.close()


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

            # Analyze file
            file_processor = FileProcessor()
            schema_info = await file_processor.analyze_file(file_path, file_format)

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


async def _process_data_directly(job_id, file_path: str, file_format: str, destination: str, destination_config: dict, schema_info: dict, db):
    """Direct data processing when Airflow is unavailable"""
    import pandas as pd
    from sqlalchemy import create_engine, text, Text
    import os

    logger.info(f"Starting direct data processing for job {job_id}")

    if file_format.lower() != 'csv':
        raise ValueError(f"Direct processing only supports CSV files, got: {file_format}")

    # Generate table name
    table_name = f"processed_data_{str(job_id).replace('-', '_')}"

    try:
        # Create synchronous database engine for pandas
        database_url = os.getenv('DATABASE_URL', '').replace('postgresql+asyncpg://', 'postgresql://')
        sync_engine = create_engine(database_url)

        # Read CSV in chunks for memory efficiency
        chunk_size = 10000  # Process 10k records at a time
        total_processed = 0

        # Get CSV parameters and estimated total records from schema_info
        delimiter = schema_info.get('csv_delimiter', ',')
        encoding = schema_info.get('encoding', 'utf-8')
        estimated_total = schema_info.get('total_rows', 0)

        logger.info(f"Processing CSV file: {file_path} with delimiter '{delimiter}' and encoding '{encoding}', estimated {estimated_total} rows")

        # Read first chunk to create table schema - use dtype=str to read as strings
        first_chunk = pd.read_csv(file_path, nrows=1000, delimiter=delimiter, encoding=encoding, dtype=str, low_memory=False)

        if first_chunk.empty:
            logger.warning(f"CSV file is empty: {file_path}")
            return 0

        # Clean column names (remove spaces, special characters)
        first_chunk.columns = [col.strip().replace(' ', '_').replace('-', '_').replace('.', '_')
                              for col in first_chunk.columns]

        # Create table schema manually with TEXT columns to avoid pandas type inference
        columns_ddl = ', '.join([f'"{col}" TEXT' for col in first_chunk.columns])
        create_table_sql = f"DROP TABLE IF EXISTS {table_name}; CREATE TABLE {table_name} ({columns_ddl});"

        logger.info(f"Executing DDL with {len(first_chunk.columns)} TEXT columns")

        with sync_engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()

        logger.info(f"Created table with TEXT columns: {table_name}")

        # Process file in chunks - read all as strings to avoid type conversion errors
        chunk_iter = pd.read_csv(
            file_path,
            chunksize=chunk_size,
            delimiter=delimiter,
            encoding=encoding,
            dtype=str,
            low_memory=False
        )

        for chunk_num, chunk in enumerate(chunk_iter):
            # Clean column names to match table schema
            chunk.columns = [col.strip().replace(' ', '_').replace('-', '_').replace('.', '_')
                           for col in chunk.columns]

            # Insert chunk into database - table already has TEXT columns
            chunk.to_sql(table_name, sync_engine, if_exists='append', index=False, method='multi')

            total_processed += len(chunk)

            # Calculate progress percentage
            progress_percent = round((total_processed / estimated_total * 100), 2) if estimated_total > 0 else 0

            # Save progress every 10 chunks (100k records)
            if chunk_num % 10 == 0:
                logger.info(f"Processed {total_processed}/{estimated_total} records ({progress_percent}%) for job {job_id}")

                # Save progress record using sync engine
                try:
                    from datetime import datetime
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
                            'message': f'Loading data to {table_name}',
                            'timestamp': datetime.utcnow()
                        })
                        conn.commit()
                except Exception as progress_error:
                    logger.warning(f"Failed to save progress record: {progress_error}")

        logger.info(f"Direct processing completed: {total_processed} records processed for job {job_id}")

        # Save final progress record
        try:
            from datetime import datetime
            with sync_engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO processing_progress
                    (job_id, stage, stage_order, progress_percent, records_processed, total_records, message, timestamp)
                    VALUES (:job_id, :stage, :stage_order, :progress_percent, :records_processed, :total_records, :message, :timestamp)
                """), {
                    'job_id': job_id,
                    'stage': 'Completed',
                    'stage_order': 5,
                    'progress_percent': 100.0,
                    'records_processed': total_processed,
                    'total_records': total_processed,
                    'message': f'Successfully loaded {total_processed} records',
                    'timestamp': datetime.utcnow()
                })
                conn.commit()
                logger.info(f"Saved final progress record for job {job_id}: {total_processed} records")
        except Exception as progress_error:
            logger.warning(f"Failed to save final progress record: {progress_error}")

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

    # Start processing in background
    import threading
    thread = threading.Thread(
        target=process_file_background_wrapper,
        args=(job_id, f"/app/data/input/{job_id}_{job.filename}", job.file_type, job.destination, job.destination_config)
    )
    thread.start()

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