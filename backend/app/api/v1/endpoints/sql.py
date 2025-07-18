from typing import Any
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models.user import User
from app.api.deps import get_current_user
from app.schemas.sql import SQLQuery, SQLResult
from app.services.sql_executor import execute_query

router = APIRouter()


@router.post("/execute", response_model=SQLResult)
async def execute_sql(
    query: SQLQuery,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Any:
    try:
        result = execute_query(db, query.sql, current_user)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))