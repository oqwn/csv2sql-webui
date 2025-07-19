import pandas as pd
from typing import Dict, Optional, Any


def sanitize_column_name(column_name: str) -> str:
    """
    Standardized column name sanitization for SQL compatibility
    
    Args:
        column_name: Original column name from file
        
    Returns:
        Sanitized column name safe for SQL usage
    """
    sanitized = str(column_name).strip().lower()
    sanitized = sanitized.replace(' ', '_').replace('-', '_').replace('.', '_')
    sanitized = ''.join(c for c in sanitized if c.isalnum() or c == '_')
    
    # Ensure column name doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = f"col_{sanitized}"
    
    # Handle empty column names
    if not sanitized:
        sanitized = "unnamed_column"
    
    return sanitized


def generate_table_name_from_filename(filename: str, sheet_name: Optional[str] = None) -> str:
    """
    Generate table name from file and optional sheet name
    
    Args:
        filename: Original filename
        sheet_name: Sheet name for Excel files (optional)
        
    Returns:
        Sanitized table name
    """
    # Remove file extensions
    base_name = filename
    for ext in ['.csv', '.xlsx', '.xls']:
        if base_name.endswith(ext):
            base_name = base_name[:-len(ext)]
            break
    
    # Add sheet name if provided
    if sheet_name:
        base_name = f"{base_name}_{sheet_name}"
    
    # Sanitize table name
    table_name = base_name.lower().replace(' ', '_').replace('-', '_')
    table_name = ''.join(c for c in table_name if c.isalnum() or c == '_')
    
    # Ensure table name doesn't start with a number
    if table_name and table_name[0].isdigit():
        table_name = f"table_{table_name}"
    
    # Handle empty table names
    if not table_name:
        table_name = "imported_table"
    
    return table_name


def build_column_preview_info(series: pd.Series, original_name: str, sql_type: str) -> Dict[str, Any]:
    """
    Build standardized column preview information
    
    Args:
        series: Pandas series containing column data
        original_name: Original column name from file
        sql_type: Detected SQL type for the column
        
    Returns:
        Dictionary with column preview information
    """
    sanitized_name = sanitize_column_name(original_name)
    
    return {
        "name": sanitized_name,
        "original_name": str(original_name),
        "suggested_type": sql_type,
        "nullable": bool(series.isnull().any()),
        "unique_values": int(len(series.unique())),
        "null_count": int(series.isnull().sum()),
        "sample_values": [str(v) for v in series.dropna().head(5).tolist()] if len(series.dropna()) > 0 else []
    }