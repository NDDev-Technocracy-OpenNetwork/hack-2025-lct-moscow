from sqlalchemy import Column, Integer, String, BigInteger, DateTime, Text, JSON, DECIMAL
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
import uuid
from app.database import Base


class ProcessingJob(Base):
    __tablename__ = "processing_jobs"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(UUID(as_uuid=True), unique=True, index=True, default=uuid.uuid4)
    filename = Column(String(255), nullable=False)
    file_size = Column(BigInteger, nullable=False)
    file_format = Column(String(50), nullable=False)
    destination_type = Column(String(50), nullable=False)
    destination_config = Column(JSON, nullable=False)
    status = Column(String(50), nullable=False, default='pending')
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    error_message = Column(Text, nullable=True)
    records_processed = Column(BigInteger, default=0)
    records_total = Column(BigInteger, nullable=True)
    ddl_script = Column(Text, nullable=True)
    etl_script = Column(Text, nullable=True)
    airflow_dag_id = Column(String(255), nullable=True)
    airflow_run_id = Column(String(255), nullable=True)


class ProcessingProgress(Base):
    __tablename__ = "processing_progress"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    stage = Column(String(100), nullable=False)
    status = Column(String(50), nullable=False)
    progress_percent = Column(DECIMAL(5, 2), default=0)
    records_processed = Column(BigInteger, default=0)
    total_records = Column(BigInteger, nullable=True)
    message = Column(Text, nullable=True)
    stage_details = Column(JSON, nullable=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return f"<ProcessingProgress(job_id={self.job_id}, stage={self.stage}, status={self.status})>"