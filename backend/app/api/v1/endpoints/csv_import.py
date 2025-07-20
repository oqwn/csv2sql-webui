from typing import Any, Optional, List, Dict
from fastapi import APIRouter, File, UploadFile, HTTPException, Form
from pydantic import BaseModel
import pandas as pd
import io
import json
import re

from app.services.csv_importer import generate_create_table_sql
from app.services.type_detection import detect_column_type
from app.services.file_validation_service import validate_csv_file
from app.services.column_utils import build_column_preview_info, generate_table_name_from_filename
from app.services.local_storage import local_storage
from app.services.sql_executor import DataSourceSQLExecutor

router = APIRouter()


class ColumnConfig(BaseModel):
    name: str
    type: str
    nullable: bool = True
    primary_key: bool = False
    unique: bool = False
    default_value: Optional[str] = None


class CSVImportConfig(BaseModel):
    table_name: str
    columns: List[ColumnConfig]


@router.post("/csv/preview")
async def preview_csv(
    file: UploadFile = File(...),
    sample_size: int = Form(10),
    table_name: Optional[str] = Form(None)
) -> Any:
    """Preview CSV file contents and get column type suggestions"""
    # Validate file
    validate_csv_file(file)
    
    contents = await file.read()
    df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
    
    # Get sample data
    sample_data = df.head(sample_size).to_dict('records')
    
    # Generate table name if not provided
    if not table_name:
        table_name = generate_table_name_from_filename(file.filename)
    
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
    create_table_sql = generate_create_table_sql(df, table_name, column_types)
    
    return {
        "table_name": table_name,
        "row_count": len(df),
        "columns": columns_info,
        "sample_data": sample_data,
        "create_table_sql": create_table_sql
    }


@router.post("/csv")
async def upload_csv(
    file: UploadFile = File(...),
    table_name: Optional[str] = Form(None),
    create_table: bool = Form(True),
    detect_types: bool = Form(True),
    data_source_id: int = Form(...)
) -> Any:
    """Upload CSV file and import to database"""
    # Get data source
    data_source = local_storage.get_data_source(data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # For now, return a message indicating this needs to be implemented
    # with direct data source connection
    return {
        "status": "error",
        "message": "Direct CSV import to data source not yet implemented. Please use the preview and SQL import method."
    }


@router.post("/csv/import-with-sql")
async def import_csv_with_sql(
    file: UploadFile = File(...),
    create_table_sql: str = Form(...),
    table_name: str = Form(...),
    column_mapping: Optional[str] = Form(None),
    data_source_id: int = Form(...)
) -> Any:
    """Import CSV with custom SQL CREATE TABLE statement"""
    # Get data source
    data_source = local_storage.get_data_source(data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Validate file
    validate_csv_file(file)
    
    # Read CSV
    contents = await file.read()
    df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
    
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
                    elif isinstance(val, str):
                        # Escape single quotes
                        escaped_val = val.replace("'", "''")
                        values.append(f"'{escaped_val}'")
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
            "table_name": table_name
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/csv/batch")
async def upload_csv_batch(
    files: List[UploadFile] = File(...),
    create_table: bool = Form(True),
    detect_types: bool = Form(True),
    data_source_id: int = Form(...)
) -> Any:
    """Upload multiple CSV files in batch"""
    # Get data source
    data_source = local_storage.get_data_source(data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    results = []
    for file in files:
        try:
            # For now, just preview each file
            validate_csv_file(file)
            
            contents = await file.read()
            df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
            
            results.append({
                "filename": file.filename,
                "status": "preview",
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": list(df.columns)
            })
            
        except Exception as e:
            results.append({
                "filename": file.filename,
                "status": "error",
                "error": str(e)
            })
    
    return {
        "files_processed": len(files),
        "results": results
    }


@router.post("/csv/import-with-config")
async def import_csv_with_config(
    file: UploadFile = File(...),
    config: str = Form(...),
    data_source_id: int = Form(...)
) -> Any:
    """Import CSV with detailed column configuration"""
    # Get data source
    data_source = local_storage.get_data_source(data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Parse config
    import_config = CSVImportConfig.parse_raw(config)
    
    # Generate CREATE TABLE SQL from config
    column_defs = []
    for col in import_config.columns:
        col_def = f"{col.name} {col.type}"
        if not col.nullable:
            col_def += " NOT NULL"
        if col.primary_key:
            col_def += " PRIMARY KEY"
        elif col.unique:
            col_def += " UNIQUE"
        if col.default_value:
            col_def += f" DEFAULT {col.default_value}"
        column_defs.append(col_def)
    
    create_table_sql = f"CREATE TABLE {import_config.table_name} (\n    {',\n    '.join(column_defs)}\n)"
    
    # Use the import_with_sql endpoint logic
    validate_csv_file(file)
    
    contents = await file.read()
    df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
    
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
        
        # Insert data (similar to import_with_sql)
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
                    elif isinstance(val, str):
                        escaped_val = val.replace("'", "''")
                        values.append(f"'{escaped_val}'")
                    else:
                        values.append(str(val))
                values_list.append(f"({', '.join(values)})")
            
            insert_sql = f"INSERT INTO {import_config.table_name} ({', '.join(df.columns)}) VALUES {', '.join(values_list)}"
            
            result = await executor.execute_query(insert_sql)
            if result['error']:
                raise HTTPException(status_code=400, detail=f"Failed to insert data: {result['error']}")
            
            total_rows += len(batch)
        
        return {
            "status": "success",
            "message": f"Successfully imported {total_rows} rows to table {import_config.table_name}",
            "rows_imported": total_rows,
            "table_name": import_config.table_name
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))