import pandas as pd
import io
from fastapi import UploadFile
from sqlalchemy.orm import Session


async def import_csv_to_table(
    db: Session, 
    file: UploadFile, 
    table_name: str
) -> dict:
    contents = await file.read()
    df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
    
    if not table_name:
        filename = file.filename or "uploaded_table"
        table_name = filename.replace('.csv', '').lower().replace(' ', '_')
    
    df.to_sql(table_name, con=db.get_bind(), if_exists='replace', index=False)
    
    row_count = len(df)
    column_count = len(df.columns)
    
    return {
        "message": f"Successfully imported {row_count} rows into table '{table_name}'",
        "table_name": table_name,
        "row_count": row_count,
        "column_count": column_count,
        "columns": list(df.columns)
    }