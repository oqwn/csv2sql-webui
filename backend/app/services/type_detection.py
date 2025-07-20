import pandas as pd
from typing import Tuple


def detect_column_type(series: pd.Series) -> Tuple[str, str]:
    """
    Detect the SQL data type for a pandas Series.
    Returns a tuple of (sql_type, pandas_dtype)
    
    This is the centralized type detection logic used by both CSV and Excel imports.
    """
    # Remove null values for type detection
    non_null = series.dropna()
    
    if len(non_null) == 0:
        return "TEXT", "object"
    
    # Check for boolean
    if set(non_null.unique()).issubset({True, False, 1, 0, "true", "false", "True", "False", "TRUE", "FALSE"}):
        return "BOOLEAN", "bool"
    
    # Check if the series already contains datetime objects (from Excel/other sources)
    if pd.api.types.is_datetime64_any_dtype(series):
        # Check if it has time component
        if (series.dt.time != pd.Timestamp('00:00:00').time()).any():
            return "TIMESTAMP", "datetime64[ns]"
        else:
            return "DATE", "datetime64[ns]"
    
    # Try to parse as datetime (for string representations only)
    # Only attempt datetime parsing on string-like data, not numeric
    if not pd.api.types.is_numeric_dtype(series):
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
                    
            # Try pandas auto-detection for strings that look like dates
            # Only if all values are strings and look date-like
            if all(isinstance(val, str) for val in non_null.head(5)):
                datetime_series = pd.to_datetime(non_null, errors='raise')
                # Check if it has time component
                if (datetime_series.dt.time != pd.Timestamp('00:00:00').time()).any():
                    return "TIMESTAMP", "datetime64[ns]"
                else:
                    return "DATE", "datetime64[ns]"
        except (ValueError, TypeError):
            pass
    
    # Try to convert to numeric (after datetime check)
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
    
    # Check string length for VARCHAR vs TEXT
    max_length = non_null.astype(str).str.len().max()
    if max_length <= 255:
        return f"VARCHAR({min(max_length * 2, 255)})", "object"
    else:
        return "TEXT", "object"