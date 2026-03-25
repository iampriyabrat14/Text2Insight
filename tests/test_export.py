"""
Export tests — verifies all three formats generate non-empty valid files.
LLM calls are mocked throughout.
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch
from httpx import AsyncClient

from backend.export.models import ExportMessage, ExportSession
from backend.export.pdf_exporter import generate_pdf
from backend.export.word_exporter import generate_word
from backend.export.ppt_exporter import generate_ppt
from backend.llm.client import LLMResponse

pytestmark = pytest.mark.asyncio

_SQL_RESP = LLMResponse(
    content="SELECT region, ROUND(SUM(sale_price),2) AS revenue FROM order_items oi JOIN orders o ON oi.order_id=o.order_id GROUP BY region ORDER BY revenue DESC LIMIT 5",
    tokens_used=80, provider="groq", latency_ms=400,
)
_SUM_RESP = LLMResponse(
    content="West leads revenue at $11.7M. Central and South follow closely.",
    tokens_used=40, provider="groq", latency_ms=200,
)


def _make_export_session(n_pairs: int = 2) -> ExportSession:
    messages = []
    for i in range(n_pairs):
        messages.append(ExportMessage(
            role="user",
            content=f"Show me revenue by region #{i+1}",
            created_at=datetime(2025, 3, 1, 10, i, 0, tzinfo=timezone.utc),
        ))
        messages.append(ExportMessage(
            role="assistant",
            content=f"West leads with highest revenue (query {i+1}).",
            generated_sql="SELECT region, SUM(sale_price) FROM order_items JOIN orders ON order_items.order_id=orders.order_id GROUP BY region",
            tokens_used=120,
            cache_hit=i % 2 == 0,
            llm_provider="groq",
            created_at=datetime(2025, 3, 1, 10, i, 5, tzinfo=timezone.utc),
        ))
    return ExportSession(
        session_id="test-session-001",
        title="Sales Analysis Q1",
        username="testuser",
        user_tier="pro",
        created_at=datetime(2025, 3, 1, 10, 0, 0, tzinfo=timezone.utc),
        messages=messages,
    )


# ---------------------------------------------------------------------------
# Unit tests — generators work without HTTP
# ---------------------------------------------------------------------------

def test_pdf_generates_bytes():
    session = _make_export_session(3)
    data = generate_pdf(session)
    assert isinstance(data, bytes)
    assert len(data) > 1000
    assert data[:4] == b"%PDF"   # PDF magic bytes


def test_pdf_empty_session():
    session = _make_export_session(0)
    data = generate_pdf(session)
    assert data[:4] == b"%PDF"


def test_word_generates_bytes():
    session = _make_export_session(2)
    data = generate_word(session)
    assert isinstance(data, bytes)
    assert len(data) > 1000
    # DOCX is a ZIP: starts with PK
    assert data[:2] == b"PK"


def test_ppt_generates_bytes():
    session = _make_export_session(2)
    data = generate_ppt(session)
    assert isinstance(data, bytes)
    assert len(data) > 1000
    # PPTX is a ZIP: starts with PK
    assert data[:2] == b"PK"


def test_qa_pairs_extracted_correctly():
    session = _make_export_session(3)
    pairs = session.qa_pairs
    assert len(pairs) == 3
    assert pairs[0]["query"].startswith("Show me revenue")
    assert "SELECT" in pairs[0]["sql"]
    assert pairs[0]["tokens"] == 120


def test_total_tokens():
    session = _make_export_session(2)
    assert session.total_tokens == 240  # 2 pairs × 120


# ---------------------------------------------------------------------------
# Integration tests — via HTTP client
# ---------------------------------------------------------------------------

async def _register_and_get_session(client: AsyncClient, username: str) -> tuple[str, str]:
    """Register user, run a query, return (access_token, session_id)."""
    await client.post("/auth/register", json={
        "username": username, "email": f"{username}@test.com", "password": "password123"
    })
    r = await client.post("/auth/login", json={"username": username, "password": "password123"})
    token = r.json()["access_token"]

    with patch("backend.llm.nl_to_sql.llm_chat", new_callable=AsyncMock, return_value=_SQL_RESP), \
         patch("backend.llm.summarizer.llm_chat", new_callable=AsyncMock, return_value=_SUM_RESP):
        qr = await client.post(
            "/chat/query",
            json={"query": "Show revenue by region"},
            headers={"Authorization": f"Bearer {token}"},
        )
    session_id = qr.json()["session_id"]
    return token, session_id


async def test_export_pdf(client: AsyncClient):
    token, session_id = await _register_and_get_session(client, "pdfuser")
    r = await client.get(
        f"/export/{session_id}?format=pdf",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/pdf"
    assert r.content[:4] == b"%PDF"
    assert "attachment" in r.headers["content-disposition"]
    assert ".pdf" in r.headers["content-disposition"]


async def test_export_word(client: AsyncClient):
    token, session_id = await _register_and_get_session(client, "worduser")
    r = await client.get(
        f"/export/{session_id}?format=word",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200
    assert "wordprocessingml" in r.headers["content-type"]
    assert r.content[:2] == b"PK"
    assert ".docx" in r.headers["content-disposition"]


async def test_export_ppt(client: AsyncClient):
    token, session_id = await _register_and_get_session(client, "pptuser")
    r = await client.get(
        f"/export/{session_id}?format=ppt",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200
    assert "presentationml" in r.headers["content-type"]
    assert r.content[:2] == b"PK"
    assert ".pptx" in r.headers["content-disposition"]


async def test_export_invalid_format(client: AsyncClient):
    token, session_id = await _register_and_get_session(client, "fmtuser")
    r = await client.get(
        f"/export/{session_id}?format=xlsx",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 400
    assert "Unsupported format" in r.json()["detail"]


async def test_export_not_found(client: AsyncClient):
    await client.post("/auth/register", json={
        "username": "nfuser", "email": "nfuser@test.com", "password": "password123"
    })
    r = await client.post("/auth/login", json={"username": "nfuser", "password": "password123"})
    token = r.json()["access_token"]
    r = await client.get(
        "/export/nonexistent-session-id?format=pdf",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 404


async def test_export_requires_auth(client: AsyncClient):
    r = await client.get("/export/some-session-id?format=pdf")
    assert r.status_code == 403


async def test_export_cannot_access_other_users_session(client: AsyncClient):
    t1, sid = await _register_and_get_session(client, "owner_exp")
    await client.post("/auth/register", json={
        "username": "thief_exp", "email": "thief_exp@test.com", "password": "password123"
    })
    r = await client.post("/auth/login", json={"username": "thief_exp", "password": "password123"})
    t2 = r.json()["access_token"]
    r = await client.get(
        f"/export/{sid}?format=pdf",
        headers={"Authorization": f"Bearer {t2}"},
    )
    assert r.status_code == 404
