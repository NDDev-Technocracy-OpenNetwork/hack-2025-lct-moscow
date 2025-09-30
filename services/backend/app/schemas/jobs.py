from pydantic import BaseModel, Field, model_serializer
from typing import Optional, Dict, Any, List
from datetime import datetime


class JobBase(BaseModel):
    filename: str
    file_size: int
    file_type: str
    destination: str
    destination_config: Dict[str, Any] = {}


class JobCreate(JobBase):
    source_path: Optional[str] = None
    target_schema: Optional[str] = None
    target_table: Optional[str] = None
    priority: int = 5


class JobResponse(JobBase):
    id: int
    job_id: str
    source_path: Optional[str] = None
    target_schema: Optional[str] = None
    target_table: Optional[str] = None
    status: str
    priority: int = 5
    created_at: datetime
    updated_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    ddl_script: Optional[str] = None
    etl_script: Optional[str] = None

    @model_serializer
    def serialize_model(self) -> Dict[str, Any]:
        """Add aliases for frontend compatibility"""
        data = {
            'id': self.id,
            'job_id': self.job_id,
            'filename': self.filename,
            'file_size': self.file_size,
            'file_type': self.file_type,
            'file_format': self.file_type,  # Alias
            'destination': self.destination,
            'destination_type': self.destination,  # Alias
            'destination_config': self.destination_config,
            'source_path': self.source_path,
            'target_schema': self.target_schema,
            'target_table': self.target_table,
            'status': self.status,
            'priority': self.priority,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'error_message': self.error_message,
            'retry_count': self.retry_count,
            'max_retries': self.max_retries,
            'records_processed': 0,  # Legacy field for frontend compatibility
            'records_total': 0,  # Legacy field for frontend compatibility
            'ddl_script': self.ddl_script,
            'etl_script': self.etl_script
        }
        return data

    class Config:
        from_attributes = True


class JobStatus(BaseModel):
    job_id: str
    status: str
    records_processed: int = 0
    records_total: int = 0
    progress_percentage: float = 0.0
    current_stage: Optional[str] = None
    error_message: Optional[str] = None


class ProgressCreate(BaseModel):
    job_id: str
    stage: str
    stage_order: int = 0
    progress_percent: float = 0.0
    records_processed: int = 0
    total_records: int = 0
    bytes_processed: Optional[int] = None
    processing_rate: Optional[float] = None
    estimated_completion: Optional[datetime] = None
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = {}


class ProgressResponse(BaseModel):
    id: int
    job_id: str
    stage: str
    stage_order: int
    progress_percent: float
    records_processed: int
    total_records: int
    bytes_processed: Optional[int] = None
    processing_rate: Optional[float] = None
    estimated_completion: Optional[datetime] = None
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = {}
    timestamp: datetime

    class Config:
        from_attributes = True


class WebSocketMessage(BaseModel):
    type: str = Field(..., description="Message type: progress, status, error, etc.")
    job_id: str
    data: Dict[str, Any]
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class AirflowCallback(BaseModel):
    job_id: str
    status: str
    stage: str
    progress_percent: Optional[float] = None
    records_processed: Optional[int] = None
    total_records: Optional[int] = None
    message: Optional[str] = None
    error_message: Optional[str] = None