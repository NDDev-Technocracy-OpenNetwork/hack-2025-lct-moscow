from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime
from uuid import UUID


class JobBase(BaseModel):
    filename: str
    file_size: int
    file_format: str
    destination_type: str
    destination_config: Dict[str, Any]


class JobCreate(JobBase):
    pass


class JobResponse(JobBase):
    id: int
    job_id: UUID
    status: str
    created_at: datetime
    updated_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    records_processed: int = 0
    records_total: Optional[int] = None
    ddl_script: Optional[str] = None
    etl_script: Optional[str] = None
    airflow_dag_id: Optional[str] = None
    airflow_run_id: Optional[str] = None

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
    job_id: UUID
    stage: str
    status: str
    progress_percent: float = 0.0
    records_processed: int = 0
    total_records: Optional[int] = None
    message: Optional[str] = None
    stage_details: Optional[Dict[str, Any]] = None


class ProgressResponse(BaseModel):
    id: int
    job_id: UUID
    stage: str
    status: str
    progress_percent: float
    records_processed: int
    total_records: Optional[int] = None
    message: Optional[str] = None
    stage_details: Optional[Dict[str, Any]] = None
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