from fastapi import APIRouter
from app.api.v1.endpoints import sql, csv_import, excel_import, export, table_crud

api_router = APIRouter()

api_router.include_router(sql.router, prefix="/sql", tags=["sql"])
api_router.include_router(csv_import.router, prefix="/import", tags=["import"])
api_router.include_router(excel_import.router, prefix="/import", tags=["import"])
api_router.include_router(export.router, prefix="/export", tags=["export"])
api_router.include_router(table_crud.router, prefix="/tables", tags=["tables"])