import pandas as pd
import numpy as np
import io
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from fastapi import UploadFile
from sqlalchemy import text, Integer, Float, Boolean, DateTime, Date, Text, VARCHAR
from sqlalchemy.orm import Session


def detect_column_type(series: pd.Series) -> Tuple[str, str]:
    """
    Detect the SQL data type for a pandas Series.
    Returns a tuple of (sql_type, pandas_dtype)
    """
    # Remove null values for type detection
    non_null = series.dropna()
    
    if len(non_null) == 0:
        return "TEXT", "object"
    
    # Check for boolean
    if set(non_null.unique()).issubset({True, False, 1, 0, "true", "false", "True", "False", "TRUE", "FALSE"}):
        return "BOOLEAN", "bool"
    
    # Try to convert to numeric
    try:
        numeric_series = pd.to_numeric(non_null, errors='raise')
        if (numeric_series % 1 == 0).all():
            # Integer type
            max_val = numeric_series.max()
            min_val = numeric_series.min()
            
            # Check if values exceed BIGINT range
            if max_val > 9223372036854775807 or min_val < -9223372036854775808:
                # Use NUMERIC for values that exceed BIGINT
                return "NUMERIC", "object"
            elif min_val >= -32768 and max_val <= 32767:
                return "SMALLINT", "int16"
            elif min_val >= -2147483648 and max_val <= 2147483647:
                return "INTEGER", "int32"
            else:
                return "BIGINT", "int64"
        else:
            # Float type
            return "DOUBLE PRECISION", "float64"
    except (ValueError, TypeError, OverflowError):
        pass
    
    # Try to parse as datetime
    try:
        # First try common date formats
        date_formats = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d', 
                       '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%m/%d/%Y %H:%M:%S']
        
        for fmt in date_formats:
            try:
                pd.to_datetime(non_null, format=fmt, errors='raise')
                if '%H:%M:%S' in fmt:
                    return "TIMESTAMP", "datetime64[ns]"
                else:
                    return "DATE", "datetime64[ns]"
            except:
                continue
                
        # Try pandas auto-detection
        datetime_series = pd.to_datetime(non_null, errors='raise')
        # Check if it has time component
        if (datetime_series.dt.time != pd.Timestamp('00:00:00').time()).any():
            return "TIMESTAMP", "datetime64[ns]"
        else:
            return "DATE", "datetime64[ns]"
    except (ValueError, TypeError):
        pass
    
    # Check string length for VARCHAR vs TEXT
    max_length = non_null.astype(str).str.len().max()
    if max_length <= 255:
        return f"VARCHAR({min(max_length * 2, 255)})", "object"
    else:
        return "TEXT", "object"


def generate_create_table_sql(
    df: pd.DataFrame,
    table_name: str,
    column_types: Dict[str, str]
) -> Tuple[str, bool, Dict[str, str]]:
    """
    Generate CREATE TABLE SQL with proper id column handling
    Returns: (create_sql, has_valid_id, column_mapping)
    """
    # Build CREATE TABLE statement
    columns = []
    has_id_column = False
    column_mapping = {}  # original -> sql column name
    
    for col in df.columns:
        # Sanitize column name
        safe_col = col.strip().replace(' ', '_').replace('-', '_').lower()
        safe_col = ''.join(c for c in safe_col if c.isalnum() or c == '_')
        sql_type = column_types.get(col, "TEXT")
        
        # Check if this is an id column
        if safe_col == 'id':
            # Check if the id column contains valid integers
            try:
                id_series = df[col].dropna()
                if len(id_series) > 0:
                    id_values = pd.to_numeric(id_series, errors='coerce')
                    if not id_values.isna().any():
                        # Valid numeric id column, use as primary key with BIGINT
                        columns.append(f'"{safe_col}" BIGINT PRIMARY KEY')
                        has_id_column = True
                        column_mapping[col] = safe_col
                    else:
                        # Invalid id column, rename it
                        columns.append(f'"{safe_col}_original" {sql_type}')
                        column_mapping[col] = f'{safe_col}_original'
                else:
                    columns.append(f'"{safe_col}" BIGINT PRIMARY KEY')
                    has_id_column = True
                    column_mapping[col] = safe_col
            except:
                # If any error, rename the column
                columns.append(f'"{safe_col}_original" {sql_type}')
                column_mapping[col] = f'{safe_col}_original'
        else:
            columns.append(f'"{safe_col}" {sql_type}')
            column_mapping[col] = safe_col
    
    # Add auto-generated ID if no valid id column exists
    if not has_id_column:
        columns.insert(0, '"id" BIGSERIAL PRIMARY KEY')
    
    create_sql = f"""CREATE TABLE IF NOT EXISTS "{table_name}" (
    {', '.join(columns)}
)"""
    
    return create_sql, has_id_column, column_mapping


def create_table_from_dataframe(
    db: Session,
    df: pd.DataFrame,
    table_name: str,
    column_types: Dict[str, str]
) -> Dict[str, str]:
    """
    Create a table with the detected column types
    Returns column mapping
    """
    create_sql, _, column_mapping = generate_create_table_sql(df, table_name, column_types)
    
    # Execute the CREATE TABLE statement
    db.execute(text(create_sql))
    db.commit()
    
    return column_mapping


async def import_csv_to_table(
    db: Session, 
    file: UploadFile, 
    table_name: str,
    create_table: bool = True,
    detect_types: bool = True
) -> dict:
    contents = await file.read()
    df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
    
    if not table_name:
        filename = file.filename or "uploaded_table"
        table_name = filename.replace('.csv', '').lower().replace(' ', '_').replace('-', '_')
    
    # Detect column types
    column_types = {}
    type_conversions = {}
    
    if detect_types:
        for col in df.columns:
            sql_type, pandas_dtype = detect_column_type(df[col])
            column_types[col] = sql_type
            type_conversions[col] = pandas_dtype
            
            # Convert data types in DataFrame
            if pandas_dtype == "int16":
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int16')
            elif pandas_dtype == "int32":
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int32')
            elif pandas_dtype == "int64":
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            elif pandas_dtype == "float64":
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif pandas_dtype == "object" and sql_type == "NUMERIC":
                # Keep as string for NUMERIC type to preserve precision
                df[col] = df[col].astype(str)
            elif pandas_dtype == "bool":
                # Convert various boolean representations
                df[col] = df[col].map({
                    'true': True, 'false': False,
                    'True': True, 'False': False,
                    'TRUE': True, 'FALSE': False,
                    '1': True, '0': False,
                    1: True, 0: False
                })
            elif pandas_dtype == "datetime64[ns]":
                df[col] = pd.to_datetime(df[col], errors='coerce')
    
    if create_table and detect_types:
        # Create table with proper types
        column_mapping = create_table_from_dataframe(db, df, table_name, column_types)
        
        # Rename dataframe columns according to mapping
        df = df.rename(columns=column_mapping)
        
        # Insert data
        df.to_sql(table_name, con=db.get_bind(), if_exists='append', index=False, method='multi')
    else:
        # Use pandas default behavior
        df.to_sql(table_name, con=db.get_bind(), if_exists='replace', index=False)
    
    row_count = len(df)
    column_count = len(df.columns)
    
    # Build column info with types
    column_info = []
    if create_table and detect_types:
        # Get the actual table structure from database
        from sqlalchemy import inspect
        inspector = inspect(db.get_bind())
        actual_columns = inspector.get_columns(table_name)
        for col in actual_columns:
            column_info.append({
                "name": col['name'],
                "type": str(col['type'])
            })
    else:
        for col in df.columns:
            column_info.append({
                "name": col,
                "type": "TEXT"
            })
    
    return {
        "message": f"Successfully imported {row_count} rows into table '{table_name}'",
        "table_name": table_name,
        "row_count": row_count,
        "column_count": len(column_info),
        "columns": [c["name"] for c in column_info],
        "column_types": column_info if detect_types else None
    }