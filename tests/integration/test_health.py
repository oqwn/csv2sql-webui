import os
import httpx
import pytest


BACKEND_URL = os.getenv("BACKEND_URL", "http://backend:8000")
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://frontend")


@pytest.mark.asyncio
async def test_backend_health():
    """Test that the backend health endpoint returns successfully."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BACKEND_URL}/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "sql-webui-backend"


@pytest.mark.asyncio
async def test_frontend_loads():
    """Test that the frontend loads successfully."""
    async with httpx.AsyncClient() as client:
        response = await client.get(FRONTEND_URL)
        assert response.status_code == 200
        assert "text/html" in response.headers.get("content-type", "")


@pytest.mark.asyncio
async def test_backend_api_accessible_from_frontend():
    """Test that the backend API is accessible through the frontend proxy."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{FRONTEND_URL}/api/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"