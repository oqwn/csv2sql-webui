import pandas as pd
from typing import Dict


# Shared boolean conversion mapping
BOOLEAN_CONVERSION_MAP = {
    'true': True, 'false': False,
    'True': True, 'False': False,
    'TRUE': True, 'FALSE': False,
    '1': True, '0': False,
    1: True, 0: False,
    True: True, False: False
}


def convert_dataframe_types_from_detection(df: pd.DataFrame, column_types: Dict[str, str]) -> pd.DataFrame:
    """
    Convert DataFrame types based on detected SQL types
    
    Args:
        df: Input DataFrame
        column_types: Mapping of column names to detected SQL types
        
    Returns:
        DataFrame with converted column types
    """
    df_converted = df.copy()
    
    for col, sql_type in column_types.items():
        if col not in df_converted.columns:
            continue
            
        series = df_converted[col]
        
        # Convert based on SQL type
        if sql_type == "SMALLINT":
            df_converted[col] = pd.to_numeric(series, errors='coerce').astype('Int16')
        elif sql_type == "INTEGER":
            df_converted[col] = pd.to_numeric(series, errors='coerce').astype('Int32')
        elif sql_type == "BIGINT":
            df_converted[col] = pd.to_numeric(series, errors='coerce').astype('Int64')
        elif sql_type == "DOUBLE PRECISION":
            df_converted[col] = pd.to_numeric(series, errors='coerce')
        elif sql_type == "NUMERIC":
            # Keep as string for NUMERIC type to preserve precision
            df_converted[col] = series.astype(str)
        elif sql_type == "BOOLEAN":
            # Convert various boolean representations
            df_converted[col] = series.map(BOOLEAN_CONVERSION_MAP)
        elif sql_type in ["DATE", "TIMESTAMP"]:
            df_converted[col] = pd.to_datetime(series, errors='coerce')
        # For VARCHAR and TEXT, keep as object (string)
    
    return df_converted


def convert_boolean_column(series: pd.Series) -> pd.Series:
    """
    Convert a pandas Series to boolean using the standard mapping
    
    Args:
        series: Input pandas Series
        
    Returns:
        Series with boolean values
    """
    return series.map(BOOLEAN_CONVERSION_MAP)


def convert_numeric_column(series: pd.Series, target_type: str) -> pd.Series:
    """
    Convert a pandas Series to the specified numeric type
    
    Args:
        series: Input pandas Series
        target_type: Target pandas dtype ('Int16', 'Int32', 'Int64', 'float64')
        
    Returns:
        Series with converted numeric values
    """
    numeric_series = pd.to_numeric(series, errors='coerce')
    
    if target_type in ['Int16', 'Int32', 'Int64']:
        return numeric_series.astype(target_type)
    else:
        return numeric_series


def convert_datetime_column(series: pd.Series) -> pd.Series:
    """
    Convert a pandas Series to datetime
    
    Args:
        series: Input pandas Series
        
    Returns:
        Series with datetime values
    """
    return pd.to_datetime(series, errors='coerce')