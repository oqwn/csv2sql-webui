"""
Shared utilities for CSV and Excel import operations.
This ensures consistency between preview and actual import.
"""
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, date
from app.services.csv_importer import generate_create_table_sql


def prepare_dataframe_for_import(
    df: pd.DataFrame, 
    table_name: str,
    column_types: Optional[Dict[str, str]] = None
) -> Tuple[str, List[str], Dict[str, str], bool]:
    """
    Prepare a DataFrame for import by generating CREATE TABLE SQL and column mappings.
    
    Args:
        df: The DataFrame to import
        table_name: Target table name
        column_types: Optional dictionary of column names to SQL types
        
    Returns:
        Tuple of:
        - create_table_sql: The CREATE TABLE SQL statement
        - insert_columns: List of column names for INSERT statement
        - column_mapping: Mapping of original to SQL column names
        - has_auto_generated_id: Whether an auto-generated ID was added
    """
    if column_types is None:
        from app.services.type_detection import detect_column_type
        column_types = {}
        for col in df.columns:
            sql_type, _ = detect_column_type(df[col])
            column_types[col] = sql_type
    
    # Generate CREATE TABLE SQL and get column mapping
    create_table_sql, has_valid_id, column_mapping = generate_create_table_sql(
        df, table_name, column_types
    )
    
    # Check if auto-generated ID was added
    has_auto_generated_id = not has_valid_id
    
    # Build the column list for INSERT statements
    insert_columns = []
    for col in df.columns:
        if col in column_mapping:
            insert_columns.append(f'"{column_mapping[col]}"')
        else:
            # Fallback to sanitized name if not in mapping
            safe_col = col.strip().replace(' ', '_').replace('-', '_').lower()
            safe_col = ''.join(c for c in safe_col if c.isalnum() or c == '_')
            insert_columns.append(f'"{safe_col}"')
    
    return create_table_sql, insert_columns, column_mapping, has_auto_generated_id


def format_value_for_sql(val: Any) -> str:
    """
    Format a single value for SQL INSERT statement.
    
    Args:
        val: The value to format
        
    Returns:
        String representation suitable for SQL
    """
    if pd.isna(val):
        return "NULL"
    elif isinstance(val, bool):
        # PostgreSQL boolean values - check before numeric types
        return "TRUE" if val else "FALSE"
    elif isinstance(val, str):
        # Check if string represents a large integer
        if val.isdigit() or (val.startswith('-') and val[1:].isdigit()):
            # Keep as numeric literal (no quotes) for large integers
            return str(val)
        else:
            # Escape single quotes for text values
            escaped_val = val.replace("'", "''")
            return f"'{escaped_val}'"
    elif isinstance(val, (pd.Timestamp, datetime, date)):
        # Format datetime values with quotes
        return f"'{val}'"
    elif isinstance(val, (int, float)) and not isinstance(val, bool):
        # Handle numeric values (including large integers)
        return str(val)
    else:
        return str(val)


def generate_insert_sql(
    df: pd.DataFrame,
    table_name: str,
    insert_columns: List[str],
    batch_size: int = 1000
) -> List[str]:
    """
    Generate INSERT SQL statements for a DataFrame.
    
    Args:
        df: The DataFrame to insert
        table_name: Target table name
        insert_columns: List of column names for INSERT
        batch_size: Number of rows per INSERT statement
        
    Returns:
        List of INSERT SQL statements
    """
    insert_statements = []
    
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        
        # Generate INSERT statements
        values_list = []
        for _, row in batch.iterrows():
            values = [format_value_for_sql(val) for val in row.values]
            values_list.append(f"({', '.join(values)})")
        
        insert_sql = f"INSERT INTO {table_name} ({', '.join(insert_columns)}) VALUES {', '.join(values_list)}"
        insert_statements.append(insert_sql)
    
    return insert_statements