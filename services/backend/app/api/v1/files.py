from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List
import uuid

from app.database import get_db
from app.models.files import FileMetadata
from app.schemas.files import FileMetadataResponse

router = APIRouter()


@router.get("/{job_id}/metadata", response_model=FileMetadataResponse)
async def get_file_metadata(job_id: str, db: AsyncSession = Depends(get_db)):
    """Get file metadata for a job"""

    try:
        job_uuid = uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID format")

    result = await db.execute(
        select(FileMetadata).where(FileMetadata.job_id == job_uuid)
    )
    metadata = result.scalar_one_or_none()

    if not metadata:
        raise HTTPException(status_code=404, detail="File metadata not found")

    return FileMetadataResponse.from_attributes(metadata)


@router.get("/", response_model=List[FileMetadataResponse])
async def get_all_file_metadata(db: AsyncSession = Depends(get_db)):
    """Get all file metadata"""

    result = await db.execute(
        select(FileMetadata).order_by(FileMetadata.created_at.desc())
    )
    metadata_list = result.scalars().all()

    return [FileMetadataResponse.from_attributes(metadata) for metadata in metadata_list]