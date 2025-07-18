from typing import List, Any, Literal
from pydantic import BaseModel


class ExportRequest(BaseModel):
    data: List[List[Any]]
    columns: List[str]
    format: Literal["csv", "excel"]
    filename: str