from typing import Any, Optional, List, Dict
from fastapi import APIRouter, File, UploadFile, HTTPException, Form
import pandas as pd
import io
import json
from datetime import datetime, date

from app.services.excel_importer import get_excel_sheets
from app.services.csv_importer import generate_create_table_sql
from app.services.type_detection import detect_column_type
from app.services.file_validation_service import validate_excel_file
from app.services.column_utils import build_column_preview_info, generate_table_name_from_filename
from app.services.local_storage import local_storage
from app.services.sql_executor import DataSourceSQLExecutor

router = APIRouter()


@router.post("/excel/sheets")
async def get_sheets(
    file: UploadFile = File(...)
) -> Any:
    """Get list of sheets in Excel file"""
    validation_result = validate_excel_file(file)
    if not validation_result["valid"]:
        raise HTTPException(status_code=400, detail=validation_result["error"])
    
    contents = await file.read()
    sheets = get_excel_sheets(io.BytesIO(contents))
    
    return {
        "filename": file.filename,
        "sheets": sheets
    }


@router.post("/excel/preview")
async def preview_excel(
    file: UploadFile = File(...),
    sheet_name: Optional[str] = Form(None),
    rows: int = Form(10)
) -> Any:
    """Preview Excel file contents"""
    validate_excel_file(file)
    
    contents = await file.read()
    
    # Read Excel file
    excel_file = pd.ExcelFile(io.BytesIO(contents))
    
    # Get sheet names
    sheet_names = excel_file.sheet_names
    
    # If sheet_name not specified, use first sheet
    if not sheet_name:
        sheet_name = sheet_names[0]
    elif sheet_name not in sheet_names:
        raise HTTPException(status_code=400, detail=f"Sheet '{sheet_name}' not found in Excel file")
    
    # Read the specified sheet
    df = pd.read_excel(io.BytesIO(contents), sheet_name=sheet_name)
    
    # Get sample data
    sample_data = df.head(rows).to_dict('records')
    
    # Generate table name
    table_name = generate_table_name_from_filename(f"{file.filename}_{sheet_name}")
    
    # Detect column types
    columns_info = []
    for col in df.columns:
        column_type, type_stats = detect_column_type(df[col])
        preview_info = build_column_preview_info(
            df[col], col, column_type
        )
        columns_info.append(preview_info)
    
    # Generate SQL
    column_types = {col_info['name']: col_info['suggested_type'] for col_info in columns_info}
    create_table_sql, _, _ = generate_create_table_sql(df, table_name, column_types)
    
    return {
        "filename": file.filename,
        "sheet_name": sheet_name,
        "available_sheets": sheet_names,
        "table_name": table_name,
        "row_count": len(df),
        "columns": columns_info,
        "sample_data": sample_data,
        "create_table_sql": create_table_sql
    }


@router.post("/excel")
async def upload_excel(
    file: UploadFile = File(...),
    table_name: Optional[str] = Form(None),
    sheet_name: Optional[str] = Form(None),
    import_all_sheets: bool = Form(False),
    create_table: bool = Form(True),
    detect_types: bool = Form(True),
    data_source_id: int = Form(...)
) -> Any:
    """Upload Excel file and import to database"""
    # Get data source
    data_source = local_storage.get_data_source(data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    return {
        "status": "error",
        "message": "Direct Excel import to data source not yet implemented. Please use the preview and SQL import method."
    }


@router.post("/excel/import-with-sql")
async def import_excel_with_sql(
    file: UploadFile = File(...),
    create_table_sql: str = Form(...),
    table_name: str = Form(...),
    sheet_name: Optional[str] = Form(None),
    column_mapping: Optional[str] = Form(None),
    data_source_id: int = Form(...)
) -> Any:
    """Import Excel with custom SQL CREATE TABLE statement"""
    # Get data source
    data_source = local_storage.get_data_source(data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Validate file
    validate_excel_file(file)
    
    contents = await file.read()
    
    # Read Excel file
    if sheet_name:
        df = pd.read_excel(io.BytesIO(contents), sheet_name=sheet_name)
    else:
        df = pd.read_excel(io.BytesIO(contents))
    
    # Apply column mapping if provided
    if column_mapping:
        mapping = json.loads(column_mapping)
        df = df.rename(columns=mapping)
    
    # Create executor
    executor = DataSourceSQLExecutor(
        data_source['type'],
        data_source['connection_config']
    )
    
    try:
        # Create table
        result = await executor.execute_query(create_table_sql)
        if result['error']:
            raise HTTPException(status_code=400, detail=f"Failed to create table: {result['error']}")
        
        # Insert data in batches
        batch_size = 1000
        total_rows = 0
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            
            # Generate INSERT statements
            values_list = []
            for _, row in batch.iterrows():
                values = []
                for val in row.values:
                    if pd.isna(val):
                        values.append("NULL")
                    elif isinstance(val, bool):
                        # PostgreSQL boolean values - check before numeric types
                        values.append("TRUE" if val else "FALSE")
                    elif isinstance(val, str):
                        escaped_val = val.replace("'", "''")
                        values.append(f"'{escaped_val}'")
                    elif isinstance(val, (pd.Timestamp, datetime, date)):
                        # Format datetime values with quotes
                        values.append(f"'{val}'")
                    else:
                        values.append(str(val))
                values_list.append(f"({', '.join(values)})")
            
            insert_sql = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES {', '.join(values_list)}"
            
            result = await executor.execute_query(insert_sql)
            if result['error']:
                raise HTTPException(status_code=400, detail=f"Failed to insert data: {result['error']}")
            
            total_rows += len(batch)
        
        return {
            "status": "success",
            "message": f"Successfully imported {total_rows} rows to table {table_name}",
            "rows_imported": total_rows,
            "table_name": table_name,
            "sheet_name": sheet_name
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))