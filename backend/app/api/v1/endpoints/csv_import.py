from typing import Any, Optional, List, Dict
from fastapi import APIRouter, Depends, File, UploadFile, HTTPException, Form
from sqlalchemy.orm import Session
from sqlalchemy import text
from pydantic import BaseModel
import pandas as pd
import io
import json
import re

from app.db.session import get_db
from app.services.csv_importer import import_csv_to_table, generate_create_table_sql
from app.services.import_service import import_file_with_sql
from app.services.type_detection import detect_column_type
from app.services.file_validation_service import validate_csv_file
from app.services.column_utils import build_column_preview_info, generate_table_name_from_filename

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


class CSVPreviewResponse(BaseModel):
    columns: List[Dict[str, Any]]
    sample_data: List[Dict[str, Any]]
    total_rows: int
    create_table_sql: str
    suggested_table_name: str


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
    validate_csv_file(file.filename)
    
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


@router.post("/csv/preview", response_model=CSVPreviewResponse)
async def preview_csv(
    file: UploadFile = File(...),
    table_name: Optional[str] = Form(None),
    sample_size: int = Form(10),
) -> Any:
    """
    Preview CSV file and detect column types
    
    - **file**: CSV file to preview
    - **sample_size**: Number of rows to return as sample (default: 10)
    """
    validate_csv_file(file.filename)
    
    try:
        # Read CSV file
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
        
        # Detect column types
        columns = []
        for col in df.columns:
            series = df[col]
            sql_type, _ = detect_column_type(series)
            
            # Build column preview info using shared utility
            column_info = build_column_preview_info(series, col, sql_type)
            columns.append(column_info)
        
        # Get sample data
        sample_df = df.head(sample_size)
        sample_data = sample_df.to_dict(orient='records')
        
        # Generate suggested table name using shared utility
        if not table_name and file.filename:
            suggested_table_name = generate_table_name_from_filename(file.filename)
        else:
            suggested_table_name = table_name or "imported_table"
        
        # Build column types dict for the shared function
        column_types_dict = {}
        for col in columns:
            column_types_dict[col["original_name"]] = col["suggested_type"]
        
        # Generate CREATE TABLE SQL using shared logic
        create_table_sql, _, _ = generate_create_table_sql(df, suggested_table_name, column_types_dict)
        
        return CSVPreviewResponse(
            columns=columns,
            sample_data=sample_data,
            total_rows=len(df),
            create_table_sql=create_table_sql,
            suggested_table_name=suggested_table_name
        )
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to preview CSV: {str(e)}")


@router.post("/csv/import-with-config")
async def import_csv_with_config(
    file: UploadFile = File(...),
    config: str = Form(...),
    db: Session = Depends(get_db),
) -> Any:
    """
    Import CSV file with custom column configuration
    """
    validate_csv_file(file.filename)
    
    try:
        # Parse config from JSON string
        import json
        config_data = json.loads(config)
        parsed_config = CSVImportConfig(**config_data)
        
        # Read CSV file
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
        
        # Create table with custom configuration
        column_definitions = []
        column_mapping = {}
        
        for col_config in parsed_config.columns:
            # Map original column name to new name
            # Find the original column name from the CSV that matches this config
            original_name = col_config.name  # Default to the configured name
            for csv_col in df.columns:
                if csv_col.lower().replace(' ', '_').replace('-', '_') == col_config.name.lower():
                    original_name = csv_col
                    break
            column_mapping[original_name] = col_config.name
            
            # Build column definition
            col_def = f'"{col_config.name}" {col_config.type}'
            
            if col_config.primary_key:
                col_def += " PRIMARY KEY"
            elif not col_config.nullable:
                col_def += " NOT NULL"
            
            if col_config.unique and not col_config.primary_key:
                col_def += " UNIQUE"
            
            if col_config.default_value:
                col_def += f" DEFAULT {col_config.default_value}"
            
            column_definitions.append(col_def)
        
        # Rename columns in dataframe
        df = df.rename(columns=column_mapping)
        
        # Create table
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS "{parsed_config.table_name}" (
            {', '.join(column_definitions)}
        )
        """
        
        db.execute(text(create_table_sql))
        db.commit()
        
        # Import data
        df.to_sql(
            parsed_config.table_name,
            con=db.get_bind(),
            if_exists='append',
            index=False,
            method='multi'
        )
        
        return {
            "message": f"Successfully imported {len(df)} rows",
            "table_name": parsed_config.table_name,
            "row_count": len(df),
            "column_count": len(parsed_config.columns)
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/csv/import-with-sql")
async def import_csv_with_sql(
    file: UploadFile = File(...),
    create_table_sql: str = Form(...),
    table_name: str = Form(...),
    column_mapping: Optional[str] = Form(None),
    db: Session = Depends(get_db),
) -> Any:
    """
    Import CSV file with custom CREATE TABLE SQL
    """
    validate_csv_file(file.filename)
    
    try:
        result = await import_file_with_sql(
            db=db,
            file=file,
            create_table_sql=create_table_sql,
            table_name=table_name,
            column_mapping_json=column_mapping
        )
        return result
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/csv/batch")
async def import_csv_batch(
    files: List[UploadFile] = File(...),
    create_table: bool = Form(True),
    detect_types: bool = Form(True),
    db: Session = Depends(get_db),
) -> Any:
    """
    Import multiple CSV files to database tables
    """
    results = []
    errors = []
    used_table_names = set()
    
    for file in files:
        if not file.filename or not file.filename.endswith('.csv'):
            errors.append({
                "filename": file.filename or "unknown",
                "error": "File must be CSV format"
            })
            continue
        
        try:
            # Generate table name from filename with better sanitization
            table_name = file.filename.replace('.csv', '').lower()
            # Replace all non-alphanumeric characters with underscores
            table_name = ''.join(c if c.isalnum() else '_' for c in table_name)
            # Remove leading digits and underscores
            table_name = table_name.lstrip('0123456789_')
            # Ensure table name is not empty
            if not table_name:
                table_name = f"imported_table_{len(results) + 1}"
            # Ensure table name doesn't start with a digit
            if table_name[0].isdigit():
                table_name = f"table_{table_name}"
            
            # Ensure unique table name
            original_table_name = table_name
            counter = 1
            while table_name in used_table_names:
                table_name = f"{original_table_name}_{counter}"
                counter += 1
            used_table_names.add(table_name)
            
            result = await import_csv_to_table(
                db=db,
                file=file,
                table_name=table_name,
                create_table=create_table,
                detect_types=detect_types
            )
            
            results.append({
                "filename": file.filename,
                "table_name": result["table_name"],
                "row_count": result["row_count"],
                "column_count": result["column_count"],
                "status": "success"
            })
            
        except Exception as e:
            # Log the full error for debugging
            import traceback
            full_error = f"{str(e)}\n{traceback.format_exc()}"
            print(f"Error importing {file.filename}: {full_error}")
            
            errors.append({
                "filename": file.filename,
                "error": str(e)
            })
            
            # Rollback any partial transaction
            try:
                db.rollback()
            except:
                pass
    
    return {
        "total_files": len(files),
        "successful": len(results),
        "failed": len(errors),
        "results": results,
        "errors": errors
    }