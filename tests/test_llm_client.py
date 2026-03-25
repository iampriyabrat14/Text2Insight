"""
Unit tests for LLM client circuit breaker — no real API calls needed.
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from backend.llm.client import CircuitBreaker, CBState, LLMResponse


# ---------------------------------------------------------------------------
# Circuit breaker logic
# ---------------------------------------------------------------------------

def test_cb_starts_closed():
    cb = CircuitBreaker(threshold=3, reset_seconds=60)
    assert cb.state == CBState.CLOSED
    assert cb.allow_groq()


def test_cb_opens_after_threshold():
    cb = CircuitBreaker(threshold=3, reset_seconds=60)
    cb.record_failure()
    cb.record_failure()
    assert cb.state == CBState.CLOSED   # not yet
    cb.record_failure()
    assert cb.state == CBState.OPEN
    assert not cb.allow_groq()


def test_cb_success_resets():
    cb = CircuitBreaker(threshold=2, reset_seconds=60)
    cb.record_failure()
    cb.record_failure()
    assert cb.state == CBState.OPEN
    cb.record_success()
    assert cb.state == CBState.CLOSED
    assert cb.failure_count == 0


def test_cb_half_open_after_reset():
    import time
    cb = CircuitBreaker(threshold=1, reset_seconds=0)   # 0s reset for test speed
    cb.record_failure()
    assert cb.state == CBState.OPEN
    # Simulate time passing
    cb.opened_at = time.monotonic() - 1  # 1 second ago > 0s reset
    assert cb.allow_groq()  # should transition to HALF_OPEN
    assert cb.state == CBState.HALF_OPEN


# ---------------------------------------------------------------------------
# llm_chat routing (mocked)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_llm_chat_uses_groq_first():
    mock_resp = LLMResponse(content="SELECT 1", tokens_used=10, provider="groq", latency_ms=50)

    with patch("backend.llm.client._call_groq", new_callable=AsyncMock, return_value=mock_resp) as mock_groq, \
         patch("backend.llm.client.get_settings") as mock_settings:
        mock_settings.return_value.groq_api_key = "fake-key"
        mock_settings.return_value.openai_api_key = "fake-key"
        mock_settings.return_value.circuit_breaker_threshold = 3
        mock_settings.return_value.circuit_breaker_reset_seconds = 60

        # Reset global circuit breaker
        import backend.llm.client as llm_module
        llm_module._cb = None

        from backend.llm.client import llm_chat
        result = await llm_chat([{"role": "user", "content": "hello"}])
        assert result.provider == "groq"
        mock_groq.assert_called_once()


@pytest.mark.asyncio
async def test_llm_chat_falls_back_to_openai():
    from groq import APITimeoutError
    oai_resp = LLMResponse(content="SELECT 2", tokens_used=15, provider="openai", latency_ms=200)

    with patch("backend.llm.client._call_groq", new_callable=AsyncMock, side_effect=Exception("Groq down")), \
         patch("backend.llm.client._call_openai", new_callable=AsyncMock, return_value=oai_resp), \
         patch("backend.llm.client.get_settings") as mock_settings:
        mock_settings.return_value.groq_api_key = "fake-key"
        mock_settings.return_value.openai_api_key = "fake-key"
        mock_settings.return_value.circuit_breaker_threshold = 3
        mock_settings.return_value.circuit_breaker_reset_seconds = 60

        import backend.llm.client as llm_module
        llm_module._cb = None

        from backend.llm.client import llm_chat
        result = await llm_chat([{"role": "user", "content": "hello"}])
        assert result.provider == "openai"
