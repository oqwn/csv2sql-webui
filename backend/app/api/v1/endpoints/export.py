from typing import Any
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
import pandas as pd
import io
from datetime import datetime

from app.db.session import get_db
from app.schemas.export import ExportRequest

router = APIRouter()


@router.post("/data")
async def export_data(
    export_request: ExportRequest,
    db: Session = Depends(get_db),
) -> Any:
    try:
        # Create DataFrame from the data
        df = pd.DataFrame(export_request.data, columns=export_request.columns)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{export_request.filename}_{timestamp}"
        
        if export_request.format == "csv":
            # Export to CSV
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            buffer.seek(0)
            
            return StreamingResponse(
                iter([buffer.getvalue()]),
                media_type="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename={filename}.csv"
                }
            )
        
        elif export_request.format == "excel":
            # Export to Excel
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Data')
            buffer.seek(0)
            
            return StreamingResponse(
                iter([buffer.getvalue()]),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={
                    "Content-Disposition": f"attachment; filename={filename}.xlsx"
                }
            )
        
        else:
            raise HTTPException(status_code=400, detail="Invalid export format")
            
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))