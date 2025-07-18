from typing import Any, Optional
from fastapi import APIRouter, Depends, File, UploadFile, HTTPException
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.services.csv_importer import import_csv_to_table

router = APIRouter()


@router.post("/csv")
async def import_csv(
    file: UploadFile = File(...),
    table_name: Optional[str] = None,
    db: Session = Depends(get_db),
) -> Any:
    if file.filename and not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be CSV format")
    
    try:
        result = await import_csv_to_table(db, file, table_name or "")
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))