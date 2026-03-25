"""
LLM client — Groq primary, OpenAI fallback, with circuit breaker.

Circuit breaker states:
  CLOSED  → normal, all requests go to Groq
  OPEN    → Groq failed threshold times; all requests routed to OpenAI for reset_seconds
  HALF_OPEN → after reset_seconds, one probe request tries Groq again

Usage:
    response = await llm_chat(messages, token_counter=True)
    # returns LLMResponse(content, tokens_used, provider, latency_ms)
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from groq import AsyncGroq, APIStatusError as GroqStatusError, APITimeoutError as GroqTimeoutError
from openai import AsyncOpenAI, APIStatusError as OAIStatusError, APITimeoutError as OAITimeoutError

from backend.config import get_settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Response model
# ---------------------------------------------------------------------------

@dataclass
class LLMResponse:
    content: str
    tokens_used: int
    provider: str          # "groq" | "openai"
    latency_ms: float


# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------

class CBState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, threshold: int, reset_seconds: int):
        self.threshold = threshold
        self.reset_seconds = reset_seconds
        self.state = CBState.CLOSED
        self.failure_count = 0
        self.opened_at: float = 0.0

    def record_success(self) -> None:
        self.failure_count = 0
        self.state = CBState.CLOSED

    def record_failure(self) -> None:
        self.failure_count += 1
        if self.failure_count >= self.threshold:
            self.state = CBState.OPEN
            self.opened_at = time.monotonic()
            logger.warning(
                "Circuit breaker OPEN after %d failures — routing to OpenAI for %ds",
                self.failure_count, self.reset_seconds,
            )

    def allow_groq(self) -> bool:
        if self.state == CBState.CLOSED:
            return True
        if self.state == CBState.OPEN:
            if time.monotonic() - self.opened_at >= self.reset_seconds:
                self.state = CBState.HALF_OPEN
                logger.info("Circuit breaker HALF_OPEN — probing Groq")
                return True
            return False
        # HALF_OPEN: allow one probe
        return True


# ---------------------------------------------------------------------------
# Singleton clients + circuit breaker
# ---------------------------------------------------------------------------

_groq_client: AsyncGroq | None = None
_oai_client: AsyncOpenAI | None = None
_cb: CircuitBreaker | None = None


def _get_groq() -> AsyncGroq:
    global _groq_client
    if _groq_client is None:
        settings = get_settings()
        _groq_client = AsyncGroq(api_key=settings.groq_api_key, timeout=settings.llm_timeout_seconds)
    return _groq_client


def _get_oai() -> AsyncOpenAI:
    global _oai_client
    if _oai_client is None:
        settings = get_settings()
        _oai_client = AsyncOpenAI(api_key=settings.openai_api_key, timeout=settings.llm_timeout_seconds)
    return _oai_client


def _get_cb() -> CircuitBreaker:
    global _cb
    if _cb is None:
        s = get_settings()
        _cb = CircuitBreaker(s.circuit_breaker_threshold, s.circuit_breaker_reset_seconds)
    return _cb


# ---------------------------------------------------------------------------
# Core chat function
# ---------------------------------------------------------------------------

async def llm_chat(
    messages: list[dict[str, str]],
    *,
    temperature: float = 0.0,
    max_tokens: int = 1024,
    json_mode: bool = False,
) -> LLMResponse:
    """
    Send a chat request. Tries Groq first; falls back to OpenAI on failure.
    Returns LLMResponse with content, token count, provider, and latency.
    Set json_mode=True to request structured JSON output (both providers support this).
    """
    settings = get_settings()
    cb = _get_cb()

    if cb.allow_groq() and settings.groq_api_key:
        try:
            return await _call_groq(messages, temperature=temperature, max_tokens=max_tokens, json_mode=json_mode)
        except (GroqStatusError, GroqTimeoutError, asyncio.TimeoutError, Exception) as exc:
            cb.record_failure()
            logger.warning("Groq failed (%s: %s), falling back to OpenAI", type(exc).__name__, exc)
    else:
        logger.info("Circuit breaker routing request directly to OpenAI")

    if not settings.openai_api_key:
        raise RuntimeError("Groq unavailable and no OpenAI API key configured")

    try:
        result = await _call_openai(messages, temperature=temperature, max_tokens=max_tokens, json_mode=json_mode)
        # If we were in half-open and it succeeded via openai, stay open a bit longer;
        # only close the breaker on a successful Groq call.
        return result
    except (OAIStatusError, OAITimeoutError) as exc:
        raise RuntimeError(f"Both LLM providers failed. OpenAI error: {exc}") from exc


async def _call_groq(
    messages: list[dict[str, str]],
    *,
    temperature: float,
    max_tokens: int,
    json_mode: bool = False,
) -> LLMResponse:
    settings = get_settings()
    client = _get_groq()
    t0 = time.perf_counter()
    kwargs: dict[str, Any] = dict(
        model=settings.groq_model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
    )
    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}
    resp = await client.chat.completions.create(**kwargs)
    latency_ms = (time.perf_counter() - t0) * 1000
    _get_cb().record_success()
    content = resp.choices[0].message.content or ""
    tokens = resp.usage.total_tokens if resp.usage else 0
    logger.debug("Groq OK — %.0fms, %d tokens", latency_ms, tokens)
    return LLMResponse(content=content, tokens_used=tokens, provider="groq", latency_ms=round(latency_ms, 2))


async def _call_openai(
    messages: list[dict[str, str]],
    *,
    temperature: float,
    max_tokens: int,
    json_mode: bool = False,
) -> LLMResponse:
    settings = get_settings()
    client = _get_oai()
    t0 = time.perf_counter()
    kwargs: dict[str, Any] = dict(
        model=settings.openai_fallback_model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
    )
    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}
    resp = await client.chat.completions.create(**kwargs)
    latency_ms = (time.perf_counter() - t0) * 1000
    content = resp.choices[0].message.content or ""
    tokens = resp.usage.total_tokens if resp.usage else 0
    logger.debug("OpenAI OK — %.0fms, %d tokens", latency_ms, tokens)
    return LLMResponse(content=content, tokens_used=tokens, provider="openai", latency_ms=round(latency_ms, 2))


# ---------------------------------------------------------------------------
# Streaming chat (async generator)
# ---------------------------------------------------------------------------

async def llm_stream(
    messages: list[dict[str, str]],
    *,
    temperature: float = 0.3,
    max_tokens: int = 512,
):
    """
    Async generator — yields text chunks as they arrive from the LLM.
    Tries Groq first; falls back to OpenAI on connection failure.
    """
    settings = get_settings()
    cb = _get_cb()

    if cb.allow_groq() and settings.groq_api_key:
        try:
            async for chunk in _stream_groq(messages, temperature=temperature, max_tokens=max_tokens):
                yield chunk
            cb.record_success()
            return
        except Exception as exc:
            cb.record_failure()
            logger.warning("Groq stream failed (%s), falling back to OpenAI", exc)

    if not settings.openai_api_key:
        raise RuntimeError("Groq unavailable and no OpenAI API key configured")

    async for chunk in _stream_openai(messages, temperature=temperature, max_tokens=max_tokens):
        yield chunk


async def _stream_groq(messages, *, temperature, max_tokens):
    settings = get_settings()
    client = _get_groq()
    stream = await client.chat.completions.create(
        model=settings.groq_model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
        stream=True,
    )
    async for chunk in stream:
        delta = chunk.choices[0].delta.content
        if delta:
            yield delta


async def _stream_openai(messages, *, temperature, max_tokens):
    settings = get_settings()
    client = _get_oai()
    stream = await client.chat.completions.create(
        model=settings.openai_fallback_model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
        stream=True,
    )
    async for chunk in stream:
        delta = chunk.choices[0].delta.content
        if delta:
            yield delta
