from fastapi import APIRouter
from app.api.v1.endpoints import auth, users, sql, csv_import

api_router = APIRouter()

api_router.include_router(auth.router, prefix="/auth", tags=["authentication"])
api_router.include_router(users.router, prefix="/users", tags=["users"])
api_router.include_router(sql.router, prefix="/sql", tags=["sql"])
api_router.include_router(csv_import.router, prefix="/import", tags=["import"])