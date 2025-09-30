from sqlalchemy import Column, Integer, String, BigInteger, DateTime, Text, JSON, Boolean, DECIMAL
from sqlalchemy.sql import func
from app.database import Base


class FileMetadata(Base):
    __tablename__ = "file_metadata"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String(36), nullable=False, index=True)
    original_filename = Column(String(255), nullable=False)
    file_path = Column(String(500), nullable=False)
    file_size = Column(BigInteger, nullable=False)
    mime_type = Column(String(100), nullable=True)
    encoding = Column(String(50), nullable=True)
    delimiter = Column(String(10), nullable=True)
    quote_char = Column(String(1), nullable=True)
    escape_char = Column(String(1), nullable=True)
    header_row = Column(Boolean, nullable=False, default=False)
    columns_detected = Column(JSON, nullable=True, default=[])
    sample_data = Column(JSON, nullable=True, default=[])
    data_quality_score = Column(DECIMAL(3, 2), nullable=True)
    schema_validation_errors = Column(JSON, nullable=True, default=[])
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return f"<FileMetadata(job_id={self.job_id}, filename={self.original_filename})>"