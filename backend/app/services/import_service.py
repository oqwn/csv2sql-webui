import pandas as pd
import io
import re
import json
from typing import Dict, List, Optional
from fastapi import UploadFile
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.services.column_utils import sanitize_column_name






def parse_column_mapping_from_sql(create_table_sql: str, original_columns: List[str]) -> Dict[str, str]:
    """
    Parse CREATE TABLE SQL to extract column mappings
    Returns mapping from original column names to SQL column names
    """
    column_pattern = r'"([^"]+)"\s+\w+'
    sql_columns = re.findall(column_pattern, create_table_sql)
    
    # Build mapping from original to SQL columns
    mapping = {}
    for i, original_col in enumerate(original_columns):
        if i < len(sql_columns):
            mapping[original_col] = sql_columns[i]
    
    return mapping


def apply_column_mapping(df: pd.DataFrame, column_mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Apply column mapping to DataFrame and filter to only mapped columns
    """
    # Rename columns according to mapping
    df_mapped = df.rename(columns=column_mapping)
    
    # Only keep columns that were successfully mapped
    mapped_columns = [col for col in df_mapped.columns if col in column_mapping.values()]
    return df_mapped[mapped_columns]


def convert_data_for_sql_types(df: pd.DataFrame, sql_columns: List[str], create_table_sql: str) -> pd.DataFrame:
    """
    Convert DataFrame data to match the SQL column types defined in CREATE TABLE
    """
    df_converted = df.copy()
    
    # Extract column definitions from SQL
    column_definitions = re.findall(r'"([^"]+)"\s+(\w+(?:\s+\w+)*)', create_table_sql)
    column_type_map = {col_name: col_type.upper() for col_name, col_type in column_definitions}
    
    for col in df_converted.columns:
        if col not in column_type_map:
            continue
            
        sql_type = column_type_map[col]
        series = df_converted[col]
        
        # Convert data based on the SQL column type from CREATE TABLE
        if sql_type in ["SMALLINT", "INT2"]:
            df_converted[col] = pd.to_numeric(series, errors='coerce').astype('Int16')
        elif sql_type in ["INTEGER", "INT", "INT4"]:
            df_converted[col] = pd.to_numeric(series, errors='coerce').astype('Int32')
        elif sql_type in ["BIGINT", "INT8"]:
            df_converted[col] = pd.to_numeric(series, errors='coerce').astype('Int64')
        elif sql_type in ["DOUBLE PRECISION", "FLOAT8", "REAL", "FLOAT4"]:
            df_converted[col] = pd.to_numeric(series, errors='coerce')
        elif sql_type == "NUMERIC":
            # Keep as string for NUMERIC type to preserve precision
            df_converted[col] = series.astype(str)
        elif sql_type == "BOOLEAN":
            # Convert various boolean representations
            bool_map = {
                'true': True, 'false': False,
                'True': True, 'False': False,
                'TRUE': True, 'FALSE': False,
                '1': True, '0': False,
                1: True, 0: False,
                True: True, False: False
            }
            df_converted[col] = series.map(bool_map).fillna(False)
        elif sql_type in ["DATE"]:
            # Convert to date
            df_converted[col] = pd.to_datetime(series, errors='coerce').dt.date
        elif sql_type in ["TIMESTAMP", "DATETIME"]:
            df_converted[col] = pd.to_datetime(series, errors='coerce')
        elif sql_type == "BIGINT" and col.lower().find('date') != -1:
            # Handle BIGINT columns that store dates - convert to Unix timestamp
            datetime_series = pd.to_datetime(series, errors='coerce')
            df_converted[col] = (datetime_series - pd.Timestamp('1970-01-01')).dt.total_seconds().astype('Int64')
        # For VARCHAR and TEXT, keep as object (string)
    
    return df_converted


async def import_file_with_sql(
    db: Session,
    file: UploadFile,
    create_table_sql: str,
    table_name: str,
    column_mapping_json: Optional[str] = None,
    sheet_name: Optional[str] = None
) -> Dict:
    """
    Unified import function for both CSV and Excel files with custom SQL
    """
    try:
        # Validate and execute the CREATE TABLE SQL
        sql_upper = create_table_sql.strip().upper()
        if not sql_upper.startswith('CREATE TABLE'):
            raise ValueError("SQL must be a CREATE TABLE statement")
        
        # Execute the CREATE TABLE statement
        db.execute(text(create_table_sql))
        db.commit()
        
        # Read file based on type
        content = await file.read()
        if file.filename and (file.filename.endswith('.xlsx') or file.filename.endswith('.xls')):
            # Excel file
            if sheet_name:
                df = pd.read_excel(io.BytesIO(content), sheet_name=sheet_name)
            else:
                df = pd.read_excel(io.BytesIO(content))
        else:
            # CSV file
            df = pd.read_csv(io.BytesIO(content))
        
        # Parse SQL to get column information
        column_pattern = r'"([^"]+)"\s+\w+'
        sql_columns = re.findall(column_pattern, create_table_sql)
        
        # Apply column mapping
        if column_mapping_json:
            column_mapping = json.loads(column_mapping_json)
        else:
            # Auto-generate mapping based on column sanitization
            column_mapping = {}
            for i, original_col in enumerate(df.columns):
                sanitized = sanitize_column_name(str(original_col))
                
                # Handle special case where id column was renamed to id_original
                if sanitized == 'id' and 'id_original' in sql_columns and 'id' not in sql_columns:
                    column_mapping[original_col] = 'id_original'
                elif i < len(sql_columns):
                    column_mapping[original_col] = sql_columns[i]
                elif sanitized in sql_columns:
                    column_mapping[original_col] = sanitized
        
        # Apply column mapping and filter
        df_mapped = apply_column_mapping(df, column_mapping)
        
        # Convert data types to match SQL schema
        df_converted = convert_data_for_sql_types(df_mapped, sql_columns, create_table_sql)
        
        # Import data to the table
        df_converted.to_sql(
            table_name,
            con=db.get_bind(),
            if_exists='append',
            index=False,
            method='multi'
        )
        
        return {
            "message": f"Successfully imported {len(df_converted)} rows to table '{table_name}'",
            "table_name": table_name,
            "row_count": len(df_converted),
            "column_count": len(df_converted.columns)
        }
        
    except Exception as e:
        db.rollback()
        raise e