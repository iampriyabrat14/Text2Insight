import pytest
import pytest_asyncio
from httpx import AsyncClient

pytestmark = pytest.mark.asyncio


async def test_register(client: AsyncClient):
    r = await client.post("/auth/register", json={
        "username": "alice",
        "email": "alice@example.com",
        "password": "secret123",
    })
    assert r.status_code == 201
    data = r.json()
    assert "access_token" in data
    assert "refresh_token" in data
    assert data["token_type"] == "bearer"
    assert data["expires_in"] > 0


async def test_register_duplicate(client: AsyncClient):
    payload = {"username": "bob", "email": "bob@example.com", "password": "secret123"}
    await client.post("/auth/register", json=payload)
    r = await client.post("/auth/register", json=payload)
    assert r.status_code == 409


async def test_login(client: AsyncClient):
    await client.post("/auth/register", json={
        "username": "carol",
        "email": "carol@example.com",
        "password": "secret123",
    })
    r = await client.post("/auth/login", json={"username": "carol", "password": "secret123"})
    assert r.status_code == 200
    assert "access_token" in r.json()


async def test_login_wrong_password(client: AsyncClient):
    await client.post("/auth/register", json={
        "username": "dave", "email": "dave@example.com", "password": "secret123"
    })
    r = await client.post("/auth/login", json={"username": "dave", "password": "wrong"})
    assert r.status_code == 401


async def test_me(client: AsyncClient):
    reg = await client.post("/auth/register", json={
        "username": "eve", "email": "eve@example.com", "password": "secret123"
    })
    token = reg.json()["access_token"]
    r = await client.get("/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200
    data = r.json()
    assert data["username"] == "eve"
    assert data["tier"] == "free"
    assert "quota" in data
    assert data["quota"]["token_limit"] == 10000


async def test_me_invalid_token(client: AsyncClient):
    r = await client.get("/auth/me", headers={"Authorization": "Bearer invalid.token.here"})
    assert r.status_code == 401


async def test_refresh(client: AsyncClient):
    reg = await client.post("/auth/register", json={
        "username": "frank", "email": "frank@example.com", "password": "secret123"
    })
    refresh_token = reg.json()["refresh_token"]
    r = await client.post("/auth/refresh", json={"refresh_token": refresh_token})
    assert r.status_code == 200
    data = r.json()
    assert "access_token" in data
    # old refresh token is now invalid (rotation)
    r2 = await client.post("/auth/refresh", json={"refresh_token": refresh_token})
    assert r2.status_code == 401


async def test_logout(client: AsyncClient):
    reg = await client.post("/auth/register", json={
        "username": "grace", "email": "grace@example.com", "password": "secret123"
    })
    tokens = reg.json()
    r = await client.post(
        "/auth/logout",
        headers={"Authorization": f"Bearer {tokens['access_token']}"}
    )
    assert r.status_code == 204
    # refresh token should now be invalid
    r2 = await client.post("/auth/refresh", json={"refresh_token": tokens["refresh_token"]})
    assert r2.status_code == 401


async def test_register_weak_password(client: AsyncClient):
    r = await client.post("/auth/register", json={
        "username": "henry", "email": "henry@example.com", "password": "short"
    })
    assert r.status_code == 422


async def test_register_invalid_tier(client: AsyncClient):
    r = await client.post("/auth/register", json={
        "username": "irene", "email": "irene@example.com",
        "password": "secret123", "tier": "platinum"
    })
    assert r.status_code == 422
