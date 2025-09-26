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

from app.database import get_db
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
    job_id = uuid.uuid4()
    file_path = os.path.join(settings.DATA_INPUT_PATH, f"{job_id}_{file.filename}")

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

    # Create processing job record
    job = ProcessingJob(
        job_id=job_id,
        filename=file.filename,
        file_size=actual_file_size,
        file_format=file_format,
        destination_type=destination_type,
        destination_config=destination_config_dict,
        status='pending'
    )

    db.add(job)
    await db.commit()
    await db.refresh(job)

    # Start background processing
    background_tasks.add_task(process_file_background, job_id, file_path, file_format, destination_type, destination_config_dict)

    return FileUploadResponse(
        job_id=job_id,
        filename=file.filename,
        file_size=actual_file_size,
        file_format=file_format,
        message="File uploaded successfully, processing started"
    )


async def process_file_background(job_id: uuid.UUID, file_path: str, file_format: str, destination_type: str, destination_config: dict):
    """Background task for file processing"""

    async with AsyncSession(bind=get_db.__wrapped__().__next__().bind) as db:
        try:
            # Update job status
            await db.execute(
                update(ProcessingJob)
                .where(ProcessingJob.job_id == job_id)
                .values(status='analyzing', started_at=datetime.utcnow())
            )
            await db.commit()

            # Analyze file
            file_processor = FileProcessor()
            schema_info = await file_processor.analyze_file(file_path, file_format)

            # Generate DDL and ETL scripts
            ddl_generator = DDLGenerator()
            ddl_script = ddl_generator.generate_ddl(schema_info, destination_type, destination_config)
            etl_script = ddl_generator.generate_etl_script(schema_info, destination_type)

            # Update job with analysis results
            await db.execute(
                update(ProcessingJob)
                .where(ProcessingJob.job_id == job_id)
                .values(
                    ddl_script=ddl_script,
                    etl_script=etl_script,
                    records_total=schema_info.get('total_rows'),
                    status='ready_for_processing'
                )
            )

            # Save file metadata
            file_metadata = FileMetadata(
                job_id=job_id,
                original_filename=os.path.basename(file_path),
                file_path=file_path,
                file_size=os.path.getsize(file_path),
                mime_type=schema_info.get('mime_type'),
                encoding=schema_info.get('encoding'),
                columns_detected={"columns": [col.dict() for col in schema_info.get('columns', [])]},
                sample_data={"sample": schema_info.get('sample_data', [])[:5]},
                schema_info=schema_info
            )

            db.add(file_metadata)
            await db.commit()

            # Trigger Airflow DAG
            airflow_client = AirflowClient()
            dag_run_id = await airflow_client.trigger_dag(str(job_id), {
                'job_id': str(job_id),
                'file_path': file_path,
                'file_format': file_format,
                'destination_type': destination_type,
                'destination_config': destination_config
            })

            # Update job with Airflow info
            await db.execute(
                update(ProcessingJob)
                .where(ProcessingJob.job_id == job_id)
                .values(
                    status='processing',
                    airflow_dag_id='bigdata_processing_dag',
                    airflow_run_id=dag_run_id
                )
            )
            await db.commit()

            logger.info(f"File processing started for job {job_id}")

        except Exception as e:
            logger.error(f"Error processing file for job {job_id}: {e}")

            # Update job with error
            await db.execute(
                update(ProcessingJob)
                .where(ProcessingJob.job_id == job_id)
                .values(status='failed', error_message=str(e))
            )
            await db.commit()


@router.get("/", response_model=List[JobResponse])
async def get_jobs(db: AsyncSession = Depends(get_db)):
    """Get all processing jobs"""

    result = await db.execute(
        select(ProcessingJob).order_by(ProcessingJob.created_at.desc())
    )
    jobs = result.scalars().all()
    return [JobResponse.from_attributes(job) for job in jobs]


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: str, db: AsyncSession = Depends(get_db)):
    """Get specific processing job"""

    try:
        job_uuid = uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID format")

    result = await db.execute(
        select(ProcessingJob).where(ProcessingJob.job_id == job_uuid)
    )
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobResponse.from_attributes(job)


@router.get("/{job_id}/status", response_model=JobStatus)
async def get_job_status(job_id: str, db: AsyncSession = Depends(get_db)):
    """Get job processing status"""

    try:
        job_uuid = uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID format")

    result = await db.execute(
        select(ProcessingJob).where(ProcessingJob.job_id == job_uuid)
    )
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Get latest progress
    progress_result = await db.execute(
        select(ProcessingProgress)
        .where(ProcessingProgress.job_id == job_uuid)
        .order_by(ProcessingProgress.timestamp.desc())
        .limit(1)
    )
    latest_progress = progress_result.scalar_one_or_none()

    progress_percentage = 0.0
    current_stage = None

    if latest_progress:
        progress_percentage = float(latest_progress.progress_percent or 0)
        current_stage = latest_progress.stage
    elif job.records_total and job.records_processed:
        progress_percentage = round((job.records_processed / job.records_total) * 100, 2)

    return JobStatus(
        job_id=str(job.job_id),
        status=job.status,
        records_processed=job.records_processed or 0,
        records_total=job.records_total or 0,
        progress_percentage=progress_percentage,
        current_stage=current_stage,
        error_message=job.error_message
    )


@router.delete("/{job_id}")
async def cancel_job(job_id: str, db: AsyncSession = Depends(get_db)):
    """Cancel processing job"""

    try:
        job_uuid = uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID format")

    # Get job
    result = await db.execute(
        select(ProcessingJob).where(ProcessingJob.job_id == job_uuid)
    )
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Cancel Airflow DAG if running
    if job.airflow_run_id:
        airflow_client = AirflowClient()
        await airflow_client.cancel_dag_run(job.airflow_run_id)

    # Update job status
    await db.execute(
        update(ProcessingJob)
        .where(ProcessingJob.job_id == job_uuid)
        .values(status='cancelled', completed_at=datetime.utcnow())
    )
    await db.commit()

    return {"message": "Job cancelled successfully"}


@router.post("/callback/airflow")
async def airflow_callback(callback_data: AirflowCallback, db: AsyncSession = Depends(get_db)):
    """Receive callbacks from Airflow DAGs"""

    try:
        job_uuid = uuid.UUID(callback_data.job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID format")

    # Update job progress
    if callback_data.records_processed is not None:
        await db.execute(
            update(ProcessingJob)
            .where(ProcessingJob.job_id == job_uuid)
            .values(
                records_processed=callback_data.records_processed,
                status=callback_data.status,
                error_message=callback_data.error_message
            )
        )

    # Add progress record
    progress = ProcessingProgress(
        job_id=job_uuid,
        stage=callback_data.stage,
        status=callback_data.status,
        progress_percent=callback_data.progress_percent or 0,
        records_processed=callback_data.records_processed or 0,
        total_records=callback_data.total_records,
        message=callback_data.message
    )

    db.add(progress)
    await db.commit()

    # TODO: Send WebSocket notification
    # websocket_manager = app.state.websocket_manager
    # await websocket_manager.send_job_update(callback_data.job_id, callback_data.dict())

    return {"status": "callback received"}


@router.get("/{job_id}/ddl")
async def get_job_ddl(job_id: str, db: AsyncSession = Depends(get_db)):
    """Get generated DDL script for job"""

    try:
        job_uuid = uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID format")

    result = await db.execute(
        select(ProcessingJob.ddl_script, ProcessingJob.etl_script)
        .where(ProcessingJob.job_id == job_uuid)
    )
    scripts = result.first()

    if not scripts:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "job_id": job_id,
        "ddl_script": scripts.ddl_script,
        "etl_script": scripts.etl_script
    }