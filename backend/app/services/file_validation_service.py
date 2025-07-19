from typing import List
from fastapi import HTTPException


def validate_file_format(filename: str, allowed_extensions: List[str], file_type_name: str) -> None:
    """
    Validate file format against allowed extensions
    
    Args:
        filename: Name of the uploaded file
        allowed_extensions: List of allowed file extensions (e.g., ['.csv', '.xlsx'])
        file_type_name: Human-readable file type name for error messages
    
    Raises:
        HTTPException: If file format is invalid
    """
    if filename and not any(filename.endswith(ext) for ext in allowed_extensions):
        formats = ", ".join(allowed_extensions)
        raise HTTPException(
            status_code=400, 
            detail=f"File must be {file_type_name} format ({formats})"
        )


def validate_csv_file(filename: str) -> None:
    """Validate CSV file format"""
    validate_file_format(filename, ['.csv'], "CSV")


def validate_excel_file(filename: str) -> None:
    """Validate Excel file format"""
    validate_file_format(filename, ['.xlsx', '.xls'], "Excel")