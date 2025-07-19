from typing import Any, Optional
from fastapi import APIRouter, Depends, File, UploadFile, HTTPException, Form
from sqlalchemy.orm import Session
from sqlalchemy import text
import pandas as pd
import io
import re
import json

from app.db.session import get_db
from app.services.excel_importer import (
    import_excel_to_table, 
    import_excel_all_sheets,
    preview_excel_data,
    get_excel_sheets
)
from app.services.import_service import import_file_with_sql

router = APIRouter()


@router.post("/excel")
async def import_excel(
    file: UploadFile = File(...),
    table_name: Optional[str] = Form(None),
    sheet_name: Optional[str] = Form(None),
    import_all_sheets: bool = Form(False),
    create_table: bool = Form(True),
    detect_types: bool = Form(True),
    db: Session = Depends(get_db),
) -> Any:
    """
    Import Excel file to database table(s)
    
    - **file**: Excel file (.xlsx or .xls)
    - **table_name**: Target table name (optional, will be auto-generated from filename)
    - **sheet_name**: Specific sheet to import (optional, defaults to first sheet)
    - **import_all_sheets**: Import all sheets as separate tables
    - **create_table**: Create table if it doesn't exist
    - **detect_types**: Automatically detect column data types
    """
    if file.filename and not (file.filename.endswith('.xlsx') or file.filename.endswith('.xls')):
        raise HTTPException(status_code=400, detail="File must be Excel format (.xlsx or .xls)")
    
    try:
        if import_all_sheets:
            result = await import_excel_all_sheets(
                db=db,
                file=file,
                table_prefix=table_name,
                create_table=create_table,
                detect_types=detect_types
            )
        else:
            result = await import_excel_to_table(
                db=db,
                file=file,
                table_name=table_name or "",
                sheet_name=sheet_name,
                create_table=create_table,
                detect_types=detect_types
            )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/excel/preview")
async def preview_excel(
    file: UploadFile = File(...),
    sheet_name: Optional[str] = Form(None),
    rows: int = Form(10),
) -> Any:
    """
    Preview Excel file data without importing
    
    - **file**: Excel file (.xlsx or .xls)
    - **sheet_name**: Specific sheet to preview (optional, defaults to first sheet)
    - **rows**: Number of rows to preview (default: 10)
    """
    if file.filename and not (file.filename.endswith('.xlsx') or file.filename.endswith('.xls')):
        raise HTTPException(status_code=400, detail="File must be Excel format (.xlsx or .xls)")
    
    try:
        contents = await file.read()
        result = preview_excel_data(contents, sheet_name, rows)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/excel/sheets")
async def get_sheets(
    file: UploadFile = File(...),
) -> Any:
    """
    Get list of sheets in an Excel file
    
    - **file**: Excel file (.xlsx or .xls)
    """
    if file.filename and not (file.filename.endswith('.xlsx') or file.filename.endswith('.xls')):
        raise HTTPException(status_code=400, detail="File must be Excel format (.xlsx or .xls)")
    
    try:
        contents = await file.read()
        sheets = get_excel_sheets(contents)
        return {
            "sheets": sheets,
            "sheet_count": len(sheets)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/excel/import-with-sql")
async def import_excel_with_sql(
    file: UploadFile = File(...),
    create_table_sql: str = Form(...),
    table_name: str = Form(...),
    sheet_name: Optional[str] = Form(None),
    column_mapping: Optional[str] = Form(None),
    db: Session = Depends(get_db),
) -> Any:
    """
    Import Excel file with custom CREATE TABLE SQL
    """
    if file.filename and not (file.filename.endswith('.xlsx') or file.filename.endswith('.xls')):
        raise HTTPException(status_code=400, detail="File must be Excel format (.xlsx or .xls)")
    
    try:
        result = await import_file_with_sql(
            db=db,
            file=file,
            create_table_sql=create_table_sql,
            table_name=table_name,
            column_mapping_json=column_mapping,
            sheet_name=sheet_name
        )
        return result
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))