import time
from typing import cast
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.engine import CursorResult
from app.schemas.sql import SQLResult


def execute_query(db: Session, query: str) -> SQLResult:
    start_time = time.time()
    
    result = db.execute(text(query))
    # Cast to CursorResult to satisfy mypy
    cursor_result = cast(CursorResult, result)
    
    # Try to get columns and rows if the result supports it
    try:
        columns = list(cursor_result.keys())
        rows = [list(row) for row in cursor_result.fetchall()]
    except Exception:
        # If the query doesn't return rows (e.g., INSERT, UPDATE, DELETE)
        columns = []
        rows = []
    
    # Get row count, which is available for all query types
    row_count = cursor_result.rowcount if cursor_result.rowcount is not None else 0
    
    # Commit the transaction for DML/DDL operations
    # Check if it's not a SELECT query
    query_upper = query.strip().upper()
    if not query_upper.startswith('SELECT') and not query_upper.startswith('SHOW') and not query_upper.startswith('DESCRIBE'):
        db.commit()
    
    execution_time = time.time() - start_time
    
    return SQLResult(
        columns=columns,
        rows=rows,
        row_count=row_count,
        execution_time=execution_time
    )