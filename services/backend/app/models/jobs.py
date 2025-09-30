from sqlalchemy import Column, Integer, String, BigInteger, DateTime, Text, JSON, DECIMAL
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
import uuid
from app.database import Base


class ProcessingJob(Base):
    __tablename__ = "processing_jobs"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String(36), unique=True, index=True, nullable=False)
    filename = Column(String(255), nullable=False)
    file_size = Column(BigInteger, nullable=False)
    file_type = Column(String(20), nullable=False)
    destination = Column(String(50), nullable=False)
    destination_config = Column(JSON, nullable=False, default={})
    source_path = Column(String(500), nullable=True)
    target_schema = Column(String(100), nullable=True)
    target_table = Column(String(100), nullable=True)
    status = Column(String(20), nullable=False, default='pending')
    priority = Column(Integer, nullable=False, default=5)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, nullable=False, default=0)
    max_retries = Column(Integer, nullable=False, default=3)
    ddl_script = Column(Text, nullable=True)
    etl_script = Column(Text, nullable=True)


class ProcessingProgress(Base):
    __tablename__ = "processing_progress"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String(36), nullable=False, index=True)
    stage = Column(String(50), nullable=False)
    stage_order = Column(Integer, nullable=False, default=0)
    progress_percent = Column(DECIMAL(5, 2), default=0)
    records_processed = Column(BigInteger, default=0)
    total_records = Column(BigInteger, default=0)
    bytes_processed = Column(BigInteger, nullable=True)
    processing_rate = Column(DECIMAL(10, 2), nullable=True)
    estimated_completion = Column(DateTime(timezone=True), nullable=True)
    message = Column(Text, nullable=True)
    details = Column(JSON, nullable=True, default={})
    timestamp = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return f"<ProcessingProgress(job_id={self.job_id}, stage={self.stage}, progress={self.progress_percent}%)>"