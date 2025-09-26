from fastapi import APIRouter, HTTPException
from typing import Dict, Any
import asyncio
import logging

from app.schemas.destinations import DestinationConnectionTest, ConnectionTestResponse
from app.config import settings

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/available")
async def get_available_destinations():
    """Get list of available destinations"""
    return {
        "destinations": [
            {
                "type": "postgresql",
                "name": "PostgreSQL",
                "description": "Relational database for structured data",
                "config_schema": {
                    "host": "string",
                    "port": "integer",
                    "database": "string",
                    "username": "string",
                    "password": "string",
                    "schema_name": "string",
                    "table_name": "string"
                }
            },
            {
                "type": "clickhouse",
                "name": "ClickHouse",
                "description": "Columnar database for analytics",
                "config_schema": {
                    "host": "string",
                    "port": "integer",
                    "database": "string",
                    "username": "string",
                    "password": "string",
                    "table_name": "string",
                    "engine": "string",
                    "order_by": "string"
                }
            },
            {
                "type": "hdfs",
                "name": "HDFS",
                "description": "Hadoop Distributed File System for big data",
                "config_schema": {
                    "namenode_url": "string",
                    "path": "string",
                    "file_format": "string",
                    "compression": "string",
                    "partition_by": "string"
                }
            }
        ]
    }


@router.post("/test-connection", response_model=ConnectionTestResponse)
async def test_destination_connection(connection_test: DestinationConnectionTest):
    """Test connection to destination"""

    try:
        if connection_test.destination_type == "postgresql":
            return await _test_postgresql_connection(connection_test.config)
        elif connection_test.destination_type == "clickhouse":
            return await _test_clickhouse_connection(connection_test.config)
        elif connection_test.destination_type == "hdfs":
            return await _test_hdfs_connection(connection_test.config)
        else:
            raise HTTPException(status_code=400, detail="Unsupported destination type")

    except Exception as e:
        logger.error(f"Connection test failed for {connection_test.destination_type}: {e}")
        return ConnectionTestResponse(
            success=False,
            message=f"Connection test failed: {str(e)}"
        )


async def _test_postgresql_connection(config: Dict[str, Any]) -> ConnectionTestResponse:
    """Test PostgreSQL connection"""
    try:
        import asyncpg

        connection_string = f"postgresql://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"

        conn = await asyncpg.connect(connection_string)
        version = await conn.fetchval("SELECT version()")
        await conn.close()

        return ConnectionTestResponse(
            success=True,
            message="PostgreSQL connection successful",
            connection_details={"version": version}
        )

    except Exception as e:
        return ConnectionTestResponse(
            success=False,
            message=f"PostgreSQL connection failed: {str(e)}"
        )


async def _test_clickhouse_connection(config: Dict[str, Any]) -> ConnectionTestResponse:
    """Test ClickHouse connection"""
    try:
        import aiohttp

        url = f"http://{config['host']}:{config['port']}/ping"
        auth = aiohttp.BasicAuth(config['username'], config['password'])

        async with aiohttp.ClientSession(auth=auth) as session:
            async with session.get(url) as response:
                if response.status == 200:
                    return ConnectionTestResponse(
                        success=True,
                        message="ClickHouse connection successful",
                        connection_details={"status": "ok"}
                    )
                else:
                    return ConnectionTestResponse(
                        success=False,
                        message=f"ClickHouse connection failed: HTTP {response.status}"
                    )

    except Exception as e:
        return ConnectionTestResponse(
            success=False,
            message=f"ClickHouse connection failed: {str(e)}"
        )


async def _test_hdfs_connection(config: Dict[str, Any]) -> ConnectionTestResponse:
    """Test HDFS connection"""
    try:
        import aiohttp

        namenode_url = config.get('namenode_url', settings.HDFS_NAMENODE_URL)
        url = f"{namenode_url}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus"

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return ConnectionTestResponse(
                        success=True,
                        message="HDFS connection successful",
                        connection_details={"namenode": "active"}
                    )
                else:
                    return ConnectionTestResponse(
                        success=False,
                        message=f"HDFS connection failed: HTTP {response.status}"
                    )

    except Exception as e:
        return ConnectionTestResponse(
            success=False,
            message=f"HDFS connection failed: {str(e)}"
        )