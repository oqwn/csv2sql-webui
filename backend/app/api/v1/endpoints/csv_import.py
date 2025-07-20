from typing import Any, Optional, List, Dict
from fastapi import APIRouter, File, UploadFile, HTTPException, Form
from pydantic import BaseModel
import pandas as pd
import io
import json
import re
from datetime import datetime, date

from app.services.csv_importer import generate_create_table_sql
from app.services.type_detection import detect_column_type
from app.services.file_validation_service import validate_csv_file
from app.services.column_utils import build_column_preview_info, generate_table_name_from_filename
from app.services.local_storage import local_storage
from app.services.sql_executor import DataSourceSQLExecutor
from app.services.import_utils import prepare_dataframe_for_import, generate_insert_sql

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
    
    # Generate SQL using shared utility
    column_types = {col_info['name']: col_info['suggested_type'] for col_info in columns_info}
    create_table_sql, insert_columns, column_mapping, has_auto_generated_id = prepare_dataframe_for_import(
        df, table_name, column_types
    )
    
    return {
        "table_name": table_name,
        "row_count": len(df),
        "columns": columns_info,
        "sample_data": sample_data,
        "create_table_sql": create_table_sql,
        "column_mapping": column_mapping,
        "has_auto_generated_id": has_auto_generated_id,
        "insert_columns": insert_columns
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
    
    # Validate file
    validate_csv_file(file)
    
    # Read CSV with proper dtype handling to preserve large integers
    contents = await file.read()
    # First read with string types to preserve precision
    df = pd.read_csv(io.StringIO(contents.decode('utf-8')), dtype=str)
    
    # Convert columns to appropriate types while preserving large integers
    for col in df.columns:
        series = df[col]
        non_null = series.dropna()
        
        if len(non_null) > 0:
            # Try to convert to numeric, but keep as string if conversion would lose precision
            try:
                # Check if all non-null values are numeric
                numeric_series = pd.to_numeric(non_null, errors='raise')
                
                # Check if it's integer-like
                if (numeric_series % 1 == 0).all():
                    # Check for large integers that might lose precision
                    max_val = numeric_series.max()
                    min_val = numeric_series.min()
                    
                    # If within safe integer range, convert to int64
                    if max_val <= 9223372036854775807 and min_val >= -9223372036854775808:
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                    else:
                        # Keep as string to avoid precision loss
                        pass
                else:
                    # Float values
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            except (ValueError, TypeError, OverflowError):
                # Keep as string/object type
                pass
    
    # Generate table name if not provided
    if not table_name:
        table_name = generate_table_name_from_filename(file.filename)
    
    # Create executor
    executor = DataSourceSQLExecutor(
        data_source['type'],
        data_source['connection_config']
    )
    
    try:
        if create_table:
            # Detect column types if requested
            if detect_types:
                column_types = {}
                for col in df.columns:
                    column_type, _ = detect_column_type(df[col])
                    column_types[col] = column_type
            else:
                # Default to TEXT for all columns
                column_types = {col: 'TEXT' for col in df.columns}
            
            # Use shared utility to prepare table creation and data
            create_table_sql, insert_columns, column_mapping, has_auto_generated_id = prepare_dataframe_for_import(
                df, table_name, column_types
            )
            
            # Create table
            result = await executor.execute_query(create_table_sql)
            if result['error']:
                raise HTTPException(status_code=400, detail=f"Failed to create table {table_name}: {result['error']}")
        else:
            # Use shared utility to prepare data for existing table
            _, insert_columns, column_mapping, has_auto_generated_id = prepare_dataframe_for_import(
                df, table_name
            )
        
        # Generate INSERT statements using shared utility
        insert_statements = generate_insert_sql(df, table_name, insert_columns)
        
        # Execute INSERT statements
        for insert_sql in insert_statements:
            result = await executor.execute_query(insert_sql)
            if result['error']:
                raise HTTPException(status_code=400, detail=f"Failed to insert data into {table_name}: {result['error']}")
        
        return {
            "status": "success",
            "message": f"Successfully imported {len(df)} rows to table {table_name}",
            "rows_imported": len(df),
            "table_name": table_name,
            "columns": list(df.columns),
            "has_auto_generated_id": has_auto_generated_id
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


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
        
        # Use shared utility to prepare data for import
        _, insert_columns, _, _ = prepare_dataframe_for_import(df, table_name)
        
        # Generate INSERT statements using shared utility
        insert_statements = generate_insert_sql(df, table_name, insert_columns)
        
        total_rows = 0
        for insert_sql in insert_statements:
            result = await executor.execute_query(insert_sql)
            if result['error']:
                raise HTTPException(status_code=400, detail=f"Failed to insert data: {result['error']}")
        
        total_rows = len(df)
        
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
    """Upload multiple CSV files in batch and import them to database"""
    # Get data source
    data_source = local_storage.get_data_source(data_source_id)
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Create executor
    executor = DataSourceSQLExecutor(
        data_source['type'],
        data_source['connection_config']
    )
    
    results = []
    total_rows_imported = 0
    
    for file in files:
        try:
            # Validate file
            validate_csv_file(file)
            
            # Read CSV with proper dtype handling to preserve large integers
            contents = await file.read()
            # First read with string types to preserve precision
            df = pd.read_csv(io.StringIO(contents.decode('utf-8')), dtype=str)
            
            # Convert columns to appropriate types while preserving large integers
            for col in df.columns:
                series = df[col]
                non_null = series.dropna()
                
                if len(non_null) > 0:
                    # Try to convert to numeric, but keep as string if conversion would lose precision
                    try:
                        # Check if all non-null values are numeric
                        numeric_series = pd.to_numeric(non_null, errors='raise')
                        
                        # Check if it's integer-like
                        if (numeric_series % 1 == 0).all():
                            # Check for large integers that might lose precision
                            max_val = numeric_series.max()
                            min_val = numeric_series.min()
                            
                            # If within safe integer range, convert to int64
                            if max_val <= 9223372036854775807 and min_val >= -9223372036854775808:
                                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                            else:
                                # Keep as string to avoid precision loss
                                pass
                        else:
                            # Float values
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    except (ValueError, TypeError, OverflowError):
                        # Keep as string/object type
                        pass
            
            # Generate table name from filename
            table_name = generate_table_name_from_filename(file.filename)
            
            if create_table:
                # Detect column types if requested
                if detect_types:
                    column_types = {}
                    for col in df.columns:
                        column_type, _ = detect_column_type(df[col])
                        column_types[col] = column_type
                else:
                    # Default to TEXT for all columns
                    column_types = {col: 'TEXT' for col in df.columns}
                
                # Use shared utility to prepare table creation and data
                create_table_sql, insert_columns, column_mapping, has_auto_generated_id = prepare_dataframe_for_import(
                    df, table_name, column_types
                )
                
                # Create table
                result = await executor.execute_query(create_table_sql)
                if result['error']:
                    raise Exception(f"Failed to create table {table_name}: {result['error']}")
            else:
                # Use shared utility to prepare data for existing table
                _, insert_columns, column_mapping, has_auto_generated_id = prepare_dataframe_for_import(
                    df, table_name
                )
            
            # Generate INSERT statements using shared utility
            insert_statements = generate_insert_sql(df, table_name, insert_columns)
            
            # Execute INSERT statements
            for insert_sql in insert_statements:
                result = await executor.execute_query(insert_sql)
                if result['error']:
                    raise Exception(f"Failed to insert data into {table_name}: {result['error']}")
            
            rows_imported = len(df)
            total_rows_imported += rows_imported
            
            results.append({
                "filename": file.filename,
                "status": "success",
                "table_name": table_name,
                "rows_imported": rows_imported,
                "columns": list(df.columns),
                "has_auto_generated_id": has_auto_generated_id
            })
            
        except Exception as e:
            results.append({
                "filename": file.filename,
                "status": "error",
                "error": str(e)
            })
    
    successful_imports = len([r for r in results if r["status"] == "success"])
    
    return {
        "files_processed": len(files),
        "successful_imports": successful_imports,
        "total_rows_imported": total_rows_imported,
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
        
        # Use shared utility to prepare data for import
        _, insert_columns, _, _ = prepare_dataframe_for_import(df, import_config.table_name)
        
        # Generate INSERT statements using shared utility
        insert_statements = generate_insert_sql(df, import_config.table_name, insert_columns)
        
        total_rows = 0
        for insert_sql in insert_statements:
            result = await executor.execute_query(insert_sql)
            if result['error']:
                raise HTTPException(status_code=400, detail=f"Failed to insert data: {result['error']}")
        
        total_rows = len(df)
        
        return {
            "status": "success",
            "message": f"Successfully imported {total_rows} rows to table {import_config.table_name}",
            "rows_imported": total_rows,
            "table_name": import_config.table_name
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))