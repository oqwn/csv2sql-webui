import time
from typing import cast, Any
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.engine import Result
from app.models.user import User
from app.schemas.sql import SQLResult


def execute_query(db: Session, query: str, user: User) -> SQLResult:
    start_time = time.time()
    
    result = db.execute(text(query))
    
    # Try to get columns and rows if the result supports it
    try:
        columns = list(result.keys())
        rows = [list(row) for row in result.fetchall()]
    except Exception:
        # If the query doesn't return rows (e.g., INSERT, UPDATE, DELETE)
        columns = []
        rows = []
    
    # Get row count, which is available for all query types
    row_count = result.rowcount if result.rowcount is not None else 0
    
    execution_time = time.time() - start_time
    
    return SQLResult(
        columns=columns,
        rows=rows,
        row_count=row_count,
        execution_time=execution_time
    )