from typing import Any
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.db.session import get_db
from app.schemas.sql import SQLQuery, SQLResult
from app.services.sql_executor import execute_query

router = APIRouter()


@router.post("/execute", response_model=SQLResult)
async def execute_sql(
    query: SQLQuery,
    db: Session = Depends(get_db),
) -> Any:
    try:
        result = execute_query(db, query.sql)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/tables")
async def get_tables(
    db: Session = Depends(get_db),
) -> Any:
    try:
        # Get all table names from the database
        result = db.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
            )
        )
        tables = [row[0] for row in result]
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/tables/{table_name}/columns")
async def get_table_columns(
    table_name: str,
    db: Session = Depends(get_db),
) -> Any:
    try:
        # Get column information for a specific table
        result = db.execute(
            text(
                "SELECT column_name, data_type, is_nullable, column_default "
                "FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = :table_name "
                "ORDER BY ordinal_position"
            ),
            {"table_name": table_name}
        )
        columns = [
            {
                "name": row[0],
                "type": row[1],
                "nullable": row[2] == 'YES',
                "default": row[3]
            }
            for row in result
        ]
        return {"columns": columns}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))