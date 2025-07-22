from fastapi import APIRouter
from app.api.v1.endpoints import sql, csv_import, excel_import, export, table_crud, data_sources, transformations, transactions, data_quality, checkpoints, jobs, code_generation

api_router = APIRouter()

api_router.include_router(sql.router, prefix="/sql", tags=["sql"])
api_router.include_router(csv_import.router, prefix="/import", tags=["import"])
api_router.include_router(excel_import.router, prefix="/import", tags=["import"])
api_router.include_router(export.router, prefix="/export", tags=["export"])
api_router.include_router(table_crud.router, prefix="/tables", tags=["tables"])
api_router.include_router(data_sources.router, prefix="/data-sources", tags=["data-sources"])
api_router.include_router(transformations.router, prefix="/transformations", tags=["transformations"])
api_router.include_router(transactions.router, prefix="/transactions", tags=["transactions"])
api_router.include_router(data_quality.router, prefix="/data-quality", tags=["data-quality"])
api_router.include_router(checkpoints.router, prefix="/checkpoints", tags=["checkpoints"])
api_router.include_router(jobs.router, prefix="/jobs", tags=["jobs"])
api_router.include_router(code_generation.router, prefix="/code-generation", tags=["code-generation"])