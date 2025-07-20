from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
import uvicorn

from app.core.config import settings
from app.api.v1.api import api_router

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    openapi_url=f"{settings.API_V1_STR}/openapi.json"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.BACKEND_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix=settings.API_V1_STR)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Custom validation error handler to provide helpful messages for missing data_source_id"""
    errors = exc.errors()
    
    # Check if the error is about missing data_source_id
    for error in errors:
        if (error.get("type") == "missing" and 
            error.get("loc") and 
            len(error["loc"]) >= 2 and 
            error["loc"][-1] == "data_source_id"):
            
            return JSONResponse(
                status_code=400,
                content={
                    "detail": "Data source connection required. Please connect to a data source first before performing this operation.",
                    "error_type": "missing_data_source",
                    "hint": "Go to the Data Sources section and connect to a database before using SQL operations or table management features."
                }
            )
    
    # For other validation errors, return the default format
    return JSONResponse(
        status_code=422,
        content={"detail": errors}
    )


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "sql-webui-backend"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)