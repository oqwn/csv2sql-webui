from typing import Any
from fastapi import APIRouter, Depends, File, UploadFile, HTTPException
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models.user import User
from app.api.deps import get_current_user
from app.services.csv_importer import import_csv_to_table

router = APIRouter()


@router.post("/csv")
async def import_csv(
    file: UploadFile = File(...),
    table_name: str = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Any:
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be CSV format")
    
    try:
        result = await import_csv_to_table(db, file, table_name, current_user)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))