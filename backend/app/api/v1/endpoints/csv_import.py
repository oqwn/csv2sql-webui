from typing import Any, Optional, List, Dict
from fastapi import APIRouter, Depends, File, UploadFile, HTTPException, Form
from sqlalchemy.orm import Session
from pydantic import BaseModel
import pandas as pd
import io

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
            
            # Suggest column configuration
            columns.append({
                "name": col,
                "original_name": col,
                "suggested_type": sql_type,
                "nullable": series.isnull().any(),
                "unique_values": len(series.unique()),
                "null_count": series.isnull().sum(),
                "sample_values": series.dropna().head(5).tolist() if len(series.dropna()) > 0 else []
            })
        
        # Get sample data
        sample_df = df.head(sample_size)
        sample_data = sample_df.to_dict(orient='records')
        
        return CSVPreviewResponse(
            columns=columns,
            sample_data=sample_data,
            total_rows=len(df)
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