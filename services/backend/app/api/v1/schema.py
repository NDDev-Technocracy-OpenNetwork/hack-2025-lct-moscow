from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Dict, Any
import logging

from app.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/tables", response_model=List[Dict[str, Any]])
async def get_database_tables(db: AsyncSession = Depends(get_db)):
    """Get list of all tables in the database with their metadata"""
    try:
        query = text("""
        SELECT
            t.table_name,
            t.table_type,
            pg_size_pretty(pg_total_relation_size(quote_ident(t.table_name)::regclass)) as size,
            obj_description(quote_ident(t.table_name)::regclass) as description,
            (SELECT COUNT(*) FROM information_schema.columns
             WHERE table_name = t.table_name AND table_schema = 'public') as column_count
        FROM information_schema.tables t
        WHERE t.table_schema = 'public'
        AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_name;
        """)
        result = await db.execute(query)
        tables = []
        for row in result:
            tables.append({
                "name": row[0],
                "type": row[1],
                "size": row[2],
                "description": row[3],
                "column_count": row[4]
            })
        return tables
    except Exception as e:
        logger.error(f"Error fetching tables: {e}")
        return []


@router.get("/tables/{table_name}/columns", response_model=List[Dict[str, Any]])
async def get_table_columns(table_name: str, db: AsyncSession = Depends(get_db)):
    """Get columns for a specific table"""
    try:
        query = text("""
        SELECT
            column_name,
            data_type,
            character_maximum_length,
            is_nullable,
            column_default,
            ordinal_position
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = :table_name
        ORDER BY ordinal_position;
        """)
        result = await db.execute(query, {"table_name": table_name})
        columns = []
        for row in result:
            columns.append({
                "name": row[0],
                "type": row[1],
                "max_length": row[2],
                "nullable": row[3] == "YES",
                "default": row[4],
                "position": row[5]
            })
        return columns
    except Exception as e:
        logger.error(f"Error fetching columns for {table_name}: {e}")
        return []


@router.get("/tables/{table_name}/relationships", response_model=List[Dict[str, Any]])
async def get_table_relationships(table_name: str, db: AsyncSession = Depends(get_db)):
    """Get foreign key relationships for a specific table"""
    try:
        query = text("""
        SELECT
            tc.constraint_name,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = 'public'
            AND tc.table_name = :table_name;
        """)
        result = await db.execute(query, {"table_name": table_name})
        relationships = []
        for row in result:
            relationships.append({
                "constraint_name": row[0],
                "column": row[1],
                "foreign_table": row[2],
                "foreign_column": row[3]
            })
        return relationships
    except Exception as e:
        logger.error(f"Error fetching relationships for {table_name}: {e}")
        return []


@router.get("/full", response_model=Dict[str, Any])
async def get_full_schema(db: AsyncSession = Depends(get_db)):
    """Get complete database schema with tables, columns, and relationships"""
    try:
        tables_data = await get_database_tables(db)

        schema = {
            "tables": [],
            "relationships": []
        }

        for table in tables_data:
            columns = await get_table_columns(table["name"], db)
            relationships = await get_table_relationships(table["name"], db)

            schema["tables"].append({
                **table,
                "columns": columns
            })

            for rel in relationships:
                schema["relationships"].append({
                    "from_table": table["name"],
                    "to_table": rel["foreign_table"],
                    "from_column": rel["column"],
                    "to_column": rel["foreign_column"]
                })

        return schema
    except Exception as e:
        logger.error(f"Error fetching full schema: {e}")
        return {"tables": [], "relationships": []}
