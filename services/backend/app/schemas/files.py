from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from datetime import datetime
from uuid import UUID


class FileUploadResponse(BaseModel):
    job_id: UUID
    filename: str
    file_size: int
    file_format: str
    message: str


class ColumnInfo(BaseModel):
    name: str
    data_type: str
    nullable: bool = True
    max_length: Optional[int] = None
    sample_values: List[str] = []


class FileAnalysisResponse(BaseModel):
    job_id: UUID
    filename: str
    file_size: int
    file_format: str
    encoding: Optional[str] = None
    total_rows: Optional[int] = None
    columns: List[ColumnInfo] = []
    sample_data: List[Dict[str, Any]] = []
    schema_suggestions: Dict[str, Any] = {}


class FileMetadataResponse(BaseModel):
    id: int
    job_id: UUID
    original_filename: str
    file_path: str
    file_size: int
    mime_type: Optional[str] = None
    encoding: Optional[str] = None
    columns_detected: Optional[Dict[str, Any]] = None
    sample_data: Optional[Dict[str, Any]] = None
    schema_info: Optional[Dict[str, Any]] = None
    created_at: datetime

    class Config:
        from_attributes = True