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
from app.services.csv_importer import import_csv_to_table, detect_column_type

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
    if file.filename and not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be CSV format")
    
    try:
        # Read CSV file
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
        
        # Detect column types
        columns = []
        for col in df.columns:
            series = df[col]
            sql_type, _ = detect_column_type(series)
            
            # Sanitize column name for SQL
            sanitized_name = col.lower().replace(' ', '_').replace('-', '_').replace('.', '_')
            sanitized_name = ''.join(c for c in sanitized_name if c.isalnum() or c == '_')
            
            # Suggest column configuration
            columns.append({
                "name": sanitized_name,
                "original_name": col,
                "suggested_type": sql_type,
                "nullable": bool(series.isnull().any()),
                "unique_values": int(len(series.unique())),
                "null_count": int(series.isnull().sum()),
                "sample_values": [str(v) for v in series.dropna().head(5).tolist()] if len(series.dropna()) > 0 else []
            })
        
        # Get sample data
        sample_df = df.head(sample_size)
        sample_data = sample_df.to_dict(orient='records')
        
        # Generate suggested table name
        if not table_name and file.filename:
            suggested_table_name = file.filename.rsplit('.', 1)[0].lower()
            suggested_table_name = suggested_table_name.replace(' ', '_').replace('-', '_')
        else:
            suggested_table_name = table_name or "imported_table"
        
        # Generate CREATE TABLE SQL
        column_definitions = []
        has_id_column = False
        id_column_index = -1
        
        for idx, col in enumerate(columns):
            col_name = col["name"]  # This is already sanitized
            col_type = col["suggested_type"]
            
            # Check if this is an id column
            if col_name.lower() == 'id':
                has_id_column = True
                id_column_index = idx
                # Check if the id column contains sequential integers
                try:
                    id_series = df[col["original_name"]].dropna()
                    if len(id_series) > 0:
                        # Check if values are numeric and sequential
                        id_values = pd.to_numeric(id_series, errors='coerce')
                        if not id_values.isna().any():
                            # If it's already a proper sequence, use it as primary key with BIGINT
                            col_def = f'"{col_name}" BIGINT PRIMARY KEY'
                        else:
                            # If not numeric, we'll need to generate our own id
                            has_id_column = False
                            col_def = f'"{col_name}_original" {col_type}'
                    else:
                        col_def = f'"{col_name}" BIGINT PRIMARY KEY'
                except:
                    # If any error, treat as regular column
                    has_id_column = False
                    col_def = f'"{col_name}_original" {col_type}'
            else:
                # Build regular column definition
                col_def = f'"{col_name}" {col_type}'
                
                # Add NOT NULL if column has no nulls
                if not col["nullable"]:
                    col_def += " NOT NULL"
                
                # Add UNIQUE if all values are unique (but not for id column)
                if col["unique_values"] == len(df) and len(df) > 1 and col_name.lower() != 'id':
                    col_def += " UNIQUE"
            
            column_definitions.append(col_def)
        
        # Add auto-generated ID column if no suitable id exists
        if not has_id_column:
            column_definitions.insert(0, '"id" BIGSERIAL PRIMARY KEY')
        
        create_table_sql = f"""CREATE TABLE IF NOT EXISTS "{suggested_table_name}" (
    {',\n    '.join(column_definitions)}
);"""
        
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
    if file.filename and not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be CSV format")
    
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
    db: Session = Depends(get_db),
) -> Any:
    """
    Import CSV file with custom CREATE TABLE SQL
    """
    if file.filename and not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be CSV format")
    
    try:
        # Validate and execute the CREATE TABLE SQL
        # Basic validation - must be CREATE TABLE statement
        sql_upper = create_table_sql.strip().upper()
        if not sql_upper.startswith('CREATE TABLE'):
            raise HTTPException(status_code=400, detail="SQL must be a CREATE TABLE statement")
        
        # Execute the CREATE TABLE statement
        db.execute(text(create_table_sql))
        db.commit()
        
        # Read CSV file
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
        
        # Parse the CREATE TABLE SQL to understand column mappings
        # Extract column names from the SQL
        import re
        column_pattern = r'"([^"]+)"\s+\w+'
        sql_columns = re.findall(column_pattern, create_table_sql)
        
        # Clean and map CSV columns to SQL columns
        csv_to_sql_mapping = {}
        for csv_col in df.columns:
            sanitized = csv_col.lower().replace(' ', '_').replace('-', '_').replace('.', '_')
            sanitized = ''.join(c for c in sanitized if c.isalnum() or c == '_')
            
            # Handle special case where id column was renamed to id_original
            if sanitized == 'id' and 'id_original' in sql_columns and 'id' not in sql_columns:
                csv_to_sql_mapping[csv_col] = 'id_original'
            elif sanitized in sql_columns:
                csv_to_sql_mapping[csv_col] = sanitized
        
        # Rename dataframe columns to match SQL table
        df = df.rename(columns=csv_to_sql_mapping)
        
        # Only keep columns that exist in the SQL table (excluding auto-generated id)
        columns_to_keep = [col for col in df.columns if col in sql_columns]
        df = df[columns_to_keep]
        
        # Import data to the table
        df.to_sql(
            table_name,
            con=db.get_bind(),
            if_exists='append',
            index=False,
            method='multi'
        )
        
        return {
            "message": f"Successfully imported {len(df)} rows to table '{table_name}'",
            "table_name": table_name,
            "row_count": len(df),
            "column_count": len(df.columns)
        }
        
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))