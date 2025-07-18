import time
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.models.user import User
from app.schemas.sql import SQLResult


def execute_query(db: Session, query: str, user: User) -> SQLResult:
    start_time = time.time()
    
    result = db.execute(text(query))
    
    # Check if result has rows
    has_rows = result._soft_closed is False and result.returns_rows
    
    columns = list(result.keys()) if has_rows else []
    rows = [list(row) for row in result] if has_rows else []
    row_count = result.rowcount if result.rowcount is not None else 0
    
    execution_time = time.time() - start_time
    
    return SQLResult(
        columns=columns,
        rows=rows,
        row_count=row_count,
        execution_time=execution_time
    )