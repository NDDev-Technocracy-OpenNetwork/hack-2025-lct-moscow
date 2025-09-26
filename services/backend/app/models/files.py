from sqlalchemy import Column, Integer, String, BigInteger, DateTime, Text, JSON
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from app.database import Base


class FileMetadata(Base):
    __tablename__ = "file_metadata"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    original_filename = Column(String(255), nullable=False)
    file_path = Column(String(500), nullable=False)
    file_size = Column(BigInteger, nullable=False)
    mime_type = Column(String(100), nullable=True)
    encoding = Column(String(50), nullable=True)
    columns_detected = Column(JSON, nullable=True)
    sample_data = Column(JSON, nullable=True)
    schema_info = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return f"<FileMetadata(job_id={self.job_id}, filename={self.original_filename})>"