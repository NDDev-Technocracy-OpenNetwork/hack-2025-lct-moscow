from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
from enum import Enum


class DestinationType(str, Enum):
    postgresql = "postgresql"
    clickhouse = "clickhouse"
    hdfs = "hdfs"
    filesystem = "filesystem"


class DestinationConfig(BaseModel):
    """Destination configuration schema"""

    destination_type: DestinationType
    connection_params: Dict[str, Any] = Field(default_factory=dict)
    table_name: Optional[str] = None
    database: Optional[str] = None
    path: Optional[str] = None
    format: Optional[str] = "parquet"

    class Config:
        json_schema_extra = {
            "example": {
                "destination_type": "postgresql",
                "connection_params": {
                    "host": "postgres",
                    "port": 5432,
                    "user": "bigdata_user",
                    "password": "bigdata_pass"
                },
                "database": "bigdata_db",
                "table_name": "processed_data"
            }
        }


class DestinationConnectionTest(BaseModel):
    """Schema for testing destination connections"""

    destination_type: DestinationType
    connection_params: Dict[str, Any]

    class Config:
        json_schema_extra = {
            "example": {
                "destination_type": "postgresql",
                "connection_params": {
                    "host": "postgres",
                    "port": 5432,
                    "user": "bigdata_user",
                    "password": "bigdata_pass",
                    "database": "bigdata_db"
                }
            }
        }


class ConnectionTestResponse(BaseModel):
    """Response schema for connection test"""

    success: bool
    message: str
    details: Optional[Dict[str, Any]] = None

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Connection successful",
                "details": {
                    "server_version": "PostgreSQL 15.4",
                    "database": "bigdata_db"
                }
            }
        }


class DestinationResponse(BaseModel):
    """Destination response schema"""

    destination_type: str
    config: Dict[str, Any]
    status: str = "active"

    class Config:
        from_attributes = True