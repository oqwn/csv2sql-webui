from typing import List, Dict, Any
from pydantic import BaseModel


class SQLQuery(BaseModel):
    sql: str


class SQLResult(BaseModel):
    columns: List[str]
    rows: List[List[Any]]
    row_count: int
    execution_time: float