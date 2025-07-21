from typing import Any, Optional, Dict
from fastapi import APIRouter, File, UploadFile, HTTPException, Form
import pandas as pd
import io
import json

from app.services.excel_importer import get_excel_sheets
from app.services.type_detection import detect_column_type
from app.services.file_validation_service import validate_excel_file
from app.services.column_utils import build_column_preview_info, generate_table_name_from_filename
from app.services.local_storage import local_storage
from app.services.sql_executor import DataSourceSQLExecutor
from app.services.import_utils import prepare_dataframe_for_import, generate_insert_sql

router = APIRouter()


async def import_single_sheet(
    executor, 
    excel_contents, 
    sheet_name: str, 
    table_name: str, 
    create_table: bool, 
    detect_types: bool
) -> Dict[str, Any]:
    """Import a single Excel sheet to database"""
    # Read the specific sheet
    df = pd.read_excel(excel_contents, sheet_name=sheet_name)
    
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
        "table_name": table_name,
        "rows_imported": len(df),
        "columns": list(df.columns),
        "has_auto_generated_id": has_auto_generated_id
    }


@router.post("/excel/sheets")
async def get_sheets(
    file: UploadFile = File(...)
) -> Any:
    """Get list of sheets in Excel file"""
    validate_excel_file(file)
    
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
    
    # Generate SQL using shared utility
    column_types = {col_info['name']: col_info['suggested_type'] for col_info in columns_info}
    create_table_sql, insert_columns, column_mapping, has_auto_generated_id = prepare_dataframe_for_import(
        df, table_name, column_types
    )
    
    return {
        "filename": file.filename,
        "sheet_name": sheet_name,
        "available_sheets": sheet_names,
        "table_name": table_name,
        "row_count": len(df),
        "columns": columns_info,
        "sample_data": sample_data,
        "create_table_sql": create_table_sql,
        "column_mapping": column_mapping,
        "has_auto_generated_id": has_auto_generated_id,
        "insert_columns": insert_columns
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
    
    # Validate file
    validate_excel_file(file)
    
    contents = await file.read()
    
    # Read Excel file
    excel_file = pd.ExcelFile(io.BytesIO(contents))
    sheet_names = excel_file.sheet_names
    
    # Create executor
    executor = DataSourceSQLExecutor(
        data_source['type'],
        data_source['connection_config']
    )
    
    results = []
    
    try:
        if import_all_sheets:
            # Import all sheets
            for sheet in sheet_names:
                result = await import_single_sheet(
                    executor, io.BytesIO(contents), sheet, 
                    table_name or f"{generate_table_name_from_filename(file.filename)}_{sheet}",
                    create_table, detect_types
                )
                results.append({
                    "sheet_name": sheet,
                    "table_name": result["table_name"],
                    "rows_imported": result["rows_imported"],
                    "status": "success"
                })
        else:
            # Import single sheet
            target_sheet = sheet_name if sheet_name and sheet_name in sheet_names else sheet_names[0]
            target_table = table_name or generate_table_name_from_filename(f"{file.filename}_{target_sheet}")
            
            result = await import_single_sheet(
                executor, io.BytesIO(contents), target_sheet,
                target_table, create_table, detect_types
            )
            
            results.append({
                "sheet_name": target_sheet,
                "table_name": result["table_name"],
                "rows_imported": result["rows_imported"],
                "status": "success"
            })
        
        total_rows = sum(r["rows_imported"] for r in results)
        
        return {
            "status": "success",
            "message": f"Successfully imported {total_rows} rows from {len(results)} sheet(s)",
            "total_rows_imported": total_rows,
            "sheets_imported": len(results),
            "results": results
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


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
        
        # Use shared utility to prepare data for import
        _, insert_columns, _, _ = prepare_dataframe_for_import(df, table_name)
        
        # Generate INSERT statements using shared utility
        insert_statements = generate_insert_sql(df, table_name, insert_columns)
        
        total_rows = 0
        for insert_sql in insert_statements:
            result = await executor.execute_query(insert_sql)
            if result['error']:
                raise HTTPException(status_code=400, detail=f"Failed to insert data: {result['error']}")
            
            # Count rows in this batch
            total_rows += insert_sql.count('(') - insert_sql.count('()')  # Count value tuples
        
        # Better way to count total rows
        total_rows = len(df)
        
        return {
            "status": "success",
            "message": f"Successfully imported {total_rows} rows to table {table_name}",
            "rows_imported": total_rows,
            "table_name": table_name,
            "sheet_name": sheet_name
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))