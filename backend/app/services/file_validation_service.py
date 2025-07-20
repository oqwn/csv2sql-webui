from typing import List, Union
from fastapi import HTTPException, UploadFile


def validate_file_format(filename: Union[str, UploadFile], allowed_extensions: List[str], file_type_name: str) -> None:
    """
    Validate file format against allowed extensions
    
    Args:
        filename: Name of the uploaded file or UploadFile object
        allowed_extensions: List of allowed file extensions (e.g., ['.csv', '.xlsx'])
        file_type_name: Human-readable file type name for error messages
    
    Raises:
        HTTPException: If file format is invalid
    """
    # Extract filename from UploadFile object if needed
    if isinstance(filename, UploadFile):
        actual_filename = filename.filename
    else:
        actual_filename = filename
    
    # Ensure actual_filename is a string and not None
    if actual_filename and isinstance(actual_filename, str) and not any(actual_filename.endswith(ext) for ext in allowed_extensions):
        formats = ", ".join(allowed_extensions)
        raise HTTPException(
            status_code=400, 
            detail=f"File must be {file_type_name} format ({formats})"
        )


def validate_csv_file(file: Union[str, UploadFile]) -> None:
    """Validate CSV file format"""
    validate_file_format(file, ['.csv'], "CSV")


def validate_excel_file(file: Union[str, UploadFile]) -> None:
    """Validate Excel file format"""
    validate_file_format(file, ['.xlsx', '.xls'], "Excel")