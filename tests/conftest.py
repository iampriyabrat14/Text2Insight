"""
Shared pytest fixtures — in-memory SQLite, test FastAPI client.
"""
import os
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

# Use in-memory DB for tests
os.environ.setdefault("SQLITE_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("DUCKDB_PATH", "./data/sales.duckdb")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key-for-testing-only")


@pytest_asyncio.fixture
async def client():
    from backend.main import app
    from backend.database.sqlite_manager import init_db
    await init_db()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac
