from typing import Any, Optional
from fastapi import APIRouter, Depends, File, UploadFile, HTTPException, Form
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.services.csv_importer import import_csv_to_table

router = APIRouter()


@router.post("/csv")
async def import_csv(
    file: UploadFile = File(...),
    table_name: Optional[str] = Form(None),
    create_table: bool = Form(True),
    detect_types: bool = Form(True),
    db: Session = Depends(get_db),
) -> Any:
    """
    Import CSV file to database table
    
    - **file**: CSV file
    - **table_name**: Target table name (optional, will be auto-generated from filename)
    - **create_table**: Create table if it doesn't exist
    - **detect_types**: Automatically detect column data types
    """
    if file.filename and not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be CSV format")
    
    try:
        result = await import_csv_to_table(
            db=db,
            file=file,
            table_name=table_name or "",
            create_table=create_table,
            detect_types=detect_types
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))