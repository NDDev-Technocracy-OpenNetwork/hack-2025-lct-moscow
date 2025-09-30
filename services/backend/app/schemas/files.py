from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from datetime import datetime


class FileUploadResponse(BaseModel):
    job_id: str
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
    job_id: str
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
    job_id: str
    original_filename: str
    file_path: str
    file_size: int
    mime_type: Optional[str] = None
    encoding: Optional[str] = None
    delimiter: Optional[str] = None
    quote_char: Optional[str] = None
    escape_char: Optional[str] = None
    header_row: bool = False
    columns_detected: Optional[List] = None
    sample_data: Optional[List] = None
    data_quality_score: Optional[float] = None
    schema_validation_errors: Optional[List] = None
    created_at: datetime

    class Config:
        from_attributes = True