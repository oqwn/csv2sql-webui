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
            if min_val >= -32768 and max_val <= 32767:
                return "SMALLINT", "int16"
            elif min_val >= -2147483648 and max_val <= 2147483647:
                return "INTEGER", "int32"
            else:
                return "BIGINT", "int64"
        else:
            # Float type
            return "DOUBLE PRECISION", "float64"
    except (ValueError, TypeError):
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
        datetime_series = pd.to_datetime(non_null, errors='raise', infer_datetime_format=True)
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


def create_table_from_dataframe(
    db: Session,
    df: pd.DataFrame,
    table_name: str,
    column_types: Dict[str, str]
) -> None:
    """
    Create a table with the detected column types
    """
    # Build CREATE TABLE statement
    columns = []
    for col in df.columns:
        # Sanitize column name
        safe_col = col.strip().replace(' ', '_').replace('-', '_').lower()
        sql_type = column_types.get(col, "TEXT")
        columns.append(f'"{safe_col}" {sql_type}')
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        {', '.join(columns)}
    )
    """
    
    # Execute the CREATE TABLE statement
    db.execute(text(create_sql))
    db.commit()


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
                df[col] = pd.to_datetime(df[col], errors='coerce', infer_datetime_format=True)
    
    # Rename columns to be SQL-safe
    df.columns = [col.strip().replace(' ', '_').replace('-', '_').lower() for col in df.columns]
    
    if create_table and detect_types:
        # Create table with proper types
        create_table_from_dataframe(db, df, table_name, column_types)
        # Insert data
        df.to_sql(table_name, con=db.get_bind(), if_exists='append', index=False, method='multi')
    else:
        # Use pandas default behavior
        df.to_sql(table_name, con=db.get_bind(), if_exists='replace', index=False)
    
    row_count = len(df)
    column_count = len(df.columns)
    
    # Build column info with types
    column_info = []
    for orig_col, new_col in zip(column_types.keys(), df.columns):
        column_info.append({
            "name": new_col,
            "original_name": orig_col,
            "type": column_types.get(orig_col, "TEXT")
        })
    
    return {
        "message": f"Successfully imported {row_count} rows into table '{table_name}'",
        "table_name": table_name,
        "row_count": row_count,
        "column_count": column_count,
        "columns": list(df.columns),
        "column_types": column_info if detect_types else None
    }