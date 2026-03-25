"""
Chat service — orchestrates the full NL→SQL→fetch→summarize pipeline.

Pipeline:
  1. Input guardrail
  2. Token quota check
  3. Cache lookup (SQL + result + summary)
  4. NL → SQL via LLM
  5. SQL guardrail
  6. DuckDB execution
  7. Output guardrail (row cap + PII mask)
  8. Summarize via LLM
  9. Cache store
 10. Persist to SQLite (session + messages + metrics)
 11. Token deduction
"""
import asyncio
import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.auth.token_ledger import check_quota, deduct_tokens
from backend.cache.cache_manager import (
    get_result_cache, get_sql_cache, get_summary_cache,
    set_result_cache, set_sql_cache, set_summary_cache,
)
from backend.database.duckdb_manager import execute_query, get_schema
from backend.database.sqlite_manager import ChatMessage, ChatSession, User
from backend.evaluation.metrics import log_metrics
from backend.llm.guardrails import check_input, mask_pii_in_results
from backend.llm.nl_to_sql import natural_language_to_sql
from backend.llm.summarizer import summarize_result, summarize_result_stream

logger = logging.getLogger(__name__)

# Schema version — bump when data is reloaded (simple approach: hash table names)
_SCHEMA_VERSION = "v1"

# How many past messages to include as conversation context (= N/2 Q&A pairs)
_HISTORY_TURNS = 6

# ── Domain relevance check ────────────────────────────────────────────────

_DATA_TERMS = re.compile(
    r"\b(revenue|sales|order|customer|product|region|profit|margin|target|rep|"
    r"quarter|monthly|yearly|annual|trend|total|count|top|bottom|compare|percent|"
    r"chart|graph|table|data|report|analysis|metric|kpi|performance|"
    r"show|list|how\s+many|how\s+much|which|who|what|where|when|"
    r"category|segment|channel|discount|price|cost|quantity|spend|"
    r"items|orders|products|customers|reps|targets|team|hire|"
    r"best|worst|highest|lowest|average|sum|growth|rank)\b",
    re.IGNORECASE,
)

_OFF_DOMAIN_REPLY = (
    "I'm a sales data assistant — I can only help with questions about your "
    "sales data (revenue, orders, customers, products, regions, etc.).\n\n"
    "Try asking something like:"
)
_OFF_DOMAIN_SUGGESTIONS = [
    "Show total revenue by region",
    "Who are the top 5 customers by spend?",
    "Which product categories have the best margin?",
]

def _is_data_query(query: str) -> bool:
    """Return True if the query is likely related to the sales domain."""
    return bool(_DATA_TERMS.search(query))


async def _fetch_history(db: AsyncSession, session_id: str | None) -> list[dict]:
    """
    Return the last _HISTORY_TURNS messages from the session as
    [{"role": "user"|"assistant", "content": ...}] for LLM context.

    Assistant entries use the generated SQL (truncated) so the model can
    extend or modify the previous query for follow-ups like "filter by Q4".
    Returns [] for new sessions or when no session_id is given.
    """
    if not session_id:
        return []

    result = await db.execute(
        select(ChatMessage)
        .where(ChatMessage.session_id == session_id)
        .order_by(ChatMessage.created_at.desc())
        .limit(_HISTORY_TURNS)
    )
    msgs = list(reversed(result.scalars().all()))

    history: list[dict] = []
    for m in msgs:
        if m.role == "user":
            history.append({"role": "user", "content": m.content})
        elif m.role == "assistant":
            # Give the LLM the SQL it previously produced — this is the context
            # that lets it resolve references like "that query" or "add region to it".
            sql_ctx = (m.generated_sql or "").strip()
            if sql_ctx:
                history.append({"role": "assistant", "content": sql_ctx[:600]})

    return history


async def _get_or_create_session(db: AsyncSession, user: User, session_id: str | None) -> ChatSession:
    if session_id:
        result = await db.execute(
            select(ChatSession).where(
                ChatSession.id == session_id,
                ChatSession.user_id == user.id,
            )
        )
        session = result.scalar_one_or_none()
        if session:
            return session

    # Create new session
    session = ChatSession(
        id=str(uuid.uuid4()),
        user_id=user.id,
        title="New Chat",
    )
    db.add(session)
    await db.flush()
    return session


async def _save_user_message(db: AsyncSession, session_id: str, content: str) -> ChatMessage:
    msg = ChatMessage(
        id=str(uuid.uuid4()),
        session_id=session_id,
        role="user",
        content=content,
    )
    db.add(msg)
    await db.flush()
    return msg


async def _save_assistant_message(
    db: AsyncSession,
    session_id: str,
    summary: str,
    sql: str,
    result: dict,
    tokens_used: int,
    cache_hit: bool,
    llm_provider: str,
) -> ChatMessage:
    msg = ChatMessage(
        id=str(uuid.uuid4()),
        session_id=session_id,
        role="assistant",
        content=summary,
        generated_sql=sql,
        result_json=json.dumps(result, default=str),
        tokens_used=tokens_used,
        cache_hit=cache_hit,
        llm_provider=llm_provider,
    )
    db.add(msg)
    await db.flush()
    return msg


async def _update_session_title(db: AsyncSession, session: ChatSession, query: str) -> None:
    """Set session title from first user query (truncated)."""
    if session.title == "New Chat":
        session.title = query[:60] + ("…" if len(query) > 60 else "")
    session.updated_at = datetime.now(timezone.utc)


async def _off_domain_response(db: AsyncSession, user: User, query: str, session_id: str | None) -> dict:
    """Save message + return a friendly off-domain reply without running the pipeline."""
    session = await _get_or_create_session(db, user, session_id)
    await _save_user_message(db, session.id, query)
    friendly = (
        "I'm a sales data assistant and can only answer questions about your "
        "sales data — revenue, orders, customers, products, regions, and performance metrics."
    )
    asst_msg = await _save_assistant_message(
        db, session.id, friendly, "",
        {"columns": [], "row_count": 0}, 0, False, "none",
    )
    await _update_session_title(db, session, query)
    await db.commit()
    return {
        "session_id":          session.id,
        "message_id":          asst_msg.id,
        "query":               query,
        "generated_sql":       "",
        "row_count":           0,
        "columns":             [],
        "rows":                [],
        "summary":             friendly,
        "key_insights":        [],
        "follow_up_questions": _OFF_DOMAIN_SUGGESTIONS,
        "tokens_used":         0,
        "cache_hit":           False,
        "llm_provider":        "none",
        "latency_ms":          0.0,
    }


async def run_query_pipeline(
    db: AsyncSession,
    user: User,
    query: str,
    session_id: str | None,
    upload_table: str | None = None,
) -> dict:
    """
    Full pipeline. Returns a dict matching QueryResponse fields.
    Raises PermissionError for quota failures (caller returns 429).
    Off-domain and SQL errors are handled gracefully with friendly messages.
    """
    t_total_start = time.perf_counter()

    # ── 1. Input guardrail ────────────────────────────────────────────────
    guard = check_input(query)
    if not guard.passed:
        raise ValueError(f"Input rejected: {guard.reason}")

    # ── 2. Domain check — return friendly message for off-topic questions ─
    if not upload_table and not _is_data_query(query):
        logger.info("Off-domain query detected: %s", query[:80])
        return await _off_domain_response(db, user, query, session_id)

    # ── 3. Token quota check ──────────────────────────────────────────────
    has_quota, ledger = await check_quota(db, user)
    if not has_quota:
        raise PermissionError(
            f"Monthly token quota exhausted ({ledger.tokens_used}/{ledger.token_limit}). "
            "Upgrade your plan or wait for next month."
        )

    # ── 3.5. Fetch conversation history for multi-turn context ────────────
    history = await _fetch_history(db, session_id)

    # ── 4. Cache lookup (skip when history present — follow-ups must be fresh) ─
    cached_sql = get_sql_cache(query, _SCHEMA_VERSION) if not history else None
    cached_result = get_result_cache(cached_sql) if cached_sql else None
    cached_summary = get_summary_cache(cached_sql, query) if cached_sql and cached_result else None

    cache_hit = cached_sql is not None and cached_result is not None and cached_summary is not None
    llm_latency_ms = 0.0
    sql_latency_ms = 0.0
    tokens_used = 0
    llm_provider = "cache"

    key_insights = []
    follow_up_questions = []
    sql = ""
    sql_confidence = 0.0
    sql_reasoning = ""
    result = {"columns": [], "rows": [], "row_count": 0, "sql_latency_ms": 0.0}

    if cache_hit:
        sql = cached_sql
        result = cached_result
        summary = cached_summary
        logger.info("Full cache hit for query: %s", query[:60])
    else:
        try:
            # ── 5. NL → SQL ───────────────────────────────────────────────
            if upload_table:
                from backend.upload.service import get_upload_schema
                custom_schema = await get_upload_schema(upload_table)
                sql, sql_confidence, sql_reasoning, nl_resp = await natural_language_to_sql(
                    query, custom_schema=custom_schema, conversation_history=history or None
                )
            else:
                sql, sql_confidence, sql_reasoning, nl_resp = await natural_language_to_sql(
                    query, conversation_history=history or None
                )
            llm_latency_ms = nl_resp.latency_ms
            tokens_used += nl_resp.tokens_used
            llm_provider = nl_resp.provider
            set_sql_cache(query, _SCHEMA_VERSION, sql)

            # ── 6. DuckDB execution ───────────────────────────────────────
            if cached_result:
                result = cached_result
            else:
                result = await execute_query(sql)
                sql_latency_ms = result.get("sql_latency_ms", 0.0)
                set_result_cache(sql, result)

            # ── 7. Output guardrail — PII masking ─────────────────────────
            result["rows"] = mask_pii_in_results(result["rows"])

            # ── 8. Summarize ──────────────────────────────────────────────
            if cached_summary:
                summary = cached_summary
            else:
                summary, key_insights, follow_up_questions, sum_resp = await summarize_result(query, result)
                tokens_used += sum_resp.tokens_used
                llm_latency_ms += sum_resp.latency_ms
                set_summary_cache(sql, query, summary)

        except (ValueError, RuntimeError) as exc:
            # SQL generation or execution failed — return a friendly message
            logger.warning("Pipeline error for query %r: %s", query[:60], exc)
            friendly = (
                "I wasn't able to generate a valid query for that. "
                "Could you rephrase or be more specific? "
                "For example, mention the metric (revenue, orders, customers) "
                "and any filters (region, date, product category)."
            )
            session = await _get_or_create_session(db, user, session_id)
            await _save_user_message(db, session.id, query)
            asst_msg = await _save_assistant_message(
                db, session.id, friendly, "",
                {"columns": [], "row_count": 0}, 0, False, llm_provider or "none",
            )
            await _update_session_title(db, session, query)
            await db.commit()
            return {
                "session_id":          session.id,
                "message_id":          asst_msg.id,
                "query":               query,
                "generated_sql":       "",
                "row_count":           0,
                "columns":             [],
                "rows":                [],
                "summary":             friendly,
                "key_insights":        [],
                "follow_up_questions": _OFF_DOMAIN_SUGGESTIONS,
                "tokens_used":         0,
                "cache_hit":           False,
                "llm_provider":        llm_provider or "none",
                "latency_ms":          round((time.perf_counter() - t_total_start) * 1000, 2),
            }

    total_latency_ms = (time.perf_counter() - t_total_start) * 1000

    # ── 9. Persist to SQLite ──────────────────────────────────────────────
    session = await _get_or_create_session(db, user, session_id)
    await _save_user_message(db, session.id, query)
    asst_msg = await _save_assistant_message(
        db, session.id, summary, sql,
        {"columns": result["columns"], "row_count": result["row_count"]},
        tokens_used, cache_hit, llm_provider,
    )
    await _update_session_title(db, session, query)
    await db.commit()

    # ── 10. Async metrics log ─────────────────────────────────────────────
    await log_metrics(
        db,
        message_id=asst_msg.id,
        user_id=user.id,
        latency_ms=total_latency_ms,
        llm_latency_ms=llm_latency_ms,
        sql_latency_ms=sql_latency_ms,
        tokens_used=tokens_used,
        cache_hit=cache_hit,
        llm_provider=llm_provider,
        sql_valid=True,
    )

    # ── 11. Deduct tokens (skip for cache hits) ───────────────────────────
    if not cache_hit and tokens_used > 0:
        await deduct_tokens(db, user, tokens_used)

    return {
        "session_id": session.id,
        "message_id": asst_msg.id,
        "query": query,
        "generated_sql": sql,
        "sql_confidence": sql_confidence,
        "sql_reasoning": sql_reasoning,
        "row_count": result["row_count"],
        "columns": result["columns"],
        "rows": result["rows"],
        "summary": summary,
        "key_insights": key_insights,
        "follow_up_questions": follow_up_questions,
        "tokens_used": tokens_used,
        "cache_hit": cache_hit,
        "llm_provider": llm_provider,
        "latency_ms": round(total_latency_ms, 2),
    }


# ---------------------------------------------------------------------------
# Session / history helpers
# ---------------------------------------------------------------------------

async def list_sessions(db: AsyncSession, user: User) -> list[dict]:
    result = await db.execute(
        select(ChatSession)
        .where(ChatSession.user_id == user.id)
        .order_by(ChatSession.updated_at.desc())
    )
    sessions = result.scalars().all()

    out = []
    for s in sessions:
        count_result = await db.execute(
            select(func.count()).where(ChatMessage.session_id == s.id)
        )
        count = count_result.scalar() or 0
        out.append({
            "id": s.id,
            "title": s.title,
            "created_at": s.created_at,
            "updated_at": s.updated_at,
            "message_count": count,
        })
    return out


async def get_session_messages(db: AsyncSession, user: User, session_id: str) -> dict:
    result = await db.execute(
        select(ChatSession).where(
            ChatSession.id == session_id,
            ChatSession.user_id == user.id,
        )
    )
    session = result.scalar_one_or_none()
    if not session:
        return None

    msgs_result = await db.execute(
        select(ChatMessage)
        .where(ChatMessage.session_id == session_id)
        .order_by(ChatMessage.created_at)
    )
    messages = msgs_result.scalars().all()

    return {
        "id": session.id,
        "title": session.title,
        "created_at": session.created_at,
        "updated_at": session.updated_at,
        "messages": [
            {
                "id": m.id,
                "role": m.role,
                "content": m.content,
                "generated_sql": m.generated_sql,
                "row_count": None,   # stored in result_json if needed
                "tokens_used": m.tokens_used,
                "cache_hit": m.cache_hit,
                "llm_provider": m.llm_provider,
                "created_at": m.created_at,
            }
            for m in messages
        ],
    }


async def delete_session(db: AsyncSession, user: User, session_id: str) -> bool:
    result = await db.execute(
        select(ChatSession).where(
            ChatSession.id == session_id,
            ChatSession.user_id == user.id,
        )
    )
    session = result.scalar_one_or_none()
    if not session:
        return False
    await db.delete(session)
    await db.commit()
    return True


# ---------------------------------------------------------------------------
# Streaming pipeline — yields SSE-formatted strings
# ---------------------------------------------------------------------------

async def run_query_pipeline_stream(
    db: AsyncSession,
    user: User,
    query: str,
    session_id: str | None,
    upload_table: str | None = None,
):
    """
    Async generator. Yields SSE strings: ``data: {...}\\n\\n``
    Event types: status | sql | result | token | done | error
    """
    def _sse(data: dict) -> str:
        return f"data: {json.dumps(data, default=str)}\n\n"

    t_start = time.perf_counter()
    llm_provider = "none"

    # ── 1. Input guardrail ────────────────────────────────────────────────
    guard = check_input(query)
    if not guard.passed:
        yield _sse({"type": "error", "message": f"Input rejected: {guard.reason}"})
        return

    # ── 2. Domain check ───────────────────────────────────────────────────
    if not upload_table and not _is_data_query(query):
        session = await _get_or_create_session(db, user, session_id)
        await _save_user_message(db, session.id, query)
        msg = (
            "I'm a sales data assistant and can only help with questions "
            "about your sales data — revenue, orders, customers, products, "
            "regions, and performance metrics."
        )
        asst_msg = await _save_assistant_message(
            db, session.id, msg, "", {"columns": [], "row_count": 0}, 0, False, "none",
        )
        await _update_session_title(db, session, query)
        await db.commit()
        yield _sse({"type": "token", "content": msg})
        yield _sse({
            "type": "done",
            "session_id": session.id, "message_id": asst_msg.id,
            "tokens_used": 0, "cache_hit": False, "llm_provider": "none",
            "latency_ms": 0.0, "key_insights": [],
            "follow_up_questions": _OFF_DOMAIN_SUGGESTIONS,
            "columns": [], "rows": [], "row_count": 0,
        })
        return

    # ── 3. Token quota ────────────────────────────────────────────────────
    has_quota, ledger = await check_quota(db, user)
    if not has_quota:
        yield _sse({"type": "error", "message": (
            f"Monthly token quota exhausted ({ledger.tokens_used}/{ledger.token_limit}). "
            "Upgrade your plan or wait for next month."
        )})
        return

    # ── 3.5. Fetch conversation history for multi-turn context ────────────
    history = await _fetch_history(db, session_id)

    # ── 4. Cache lookup (skip when history present — follow-ups must be fresh) ─
    cached_sql    = get_sql_cache(query, _SCHEMA_VERSION) if not history else None
    cached_result = get_result_cache(cached_sql) if cached_sql else None
    cached_summary = get_summary_cache(cached_sql, query) if cached_sql and cached_result else None
    cache_hit = bool(cached_sql and cached_result and cached_summary)

    if cache_hit:
        yield _sse({"type": "sql",    "sql": cached_sql, "provider": "cache"})
        yield _sse({"type": "result",
                    "columns":   cached_result["columns"],
                    "rows":      cached_result["rows"],
                    "row_count": cached_result["row_count"]})
        # Replay cached summary word-by-word for the typing effect
        words = cached_summary.split()
        for i, word in enumerate(words):
            yield _sse({"type": "token", "content": word + (" " if i < len(words) - 1 else "")})
            await asyncio.sleep(0.018)

        session  = await _get_or_create_session(db, user, session_id)
        await _save_user_message(db, session.id, query)
        asst_msg = await _save_assistant_message(
            db, session.id, cached_summary, cached_sql,
            {"columns": cached_result["columns"], "row_count": cached_result["row_count"]},
            0, True, "cache",
        )
        await _update_session_title(db, session, query)
        await db.commit()
        yield _sse({
            "type": "done",
            "session_id": session.id, "message_id": asst_msg.id,
            "tokens_used": 0, "cache_hit": True, "llm_provider": "cache",
            "latency_ms": round((time.perf_counter() - t_start) * 1000, 2),
            "key_insights": [], "follow_up_questions": [],
            "columns": cached_result["columns"],
            "rows":    cached_result["rows"],
            "row_count": cached_result["row_count"],
        })
        return

    # ── 5. NL → SQL ───────────────────────────────────────────────────────
    yield _sse({"type": "status", "message": "Generating SQL…"})
    sql = ""
    sql_confidence = 0.0
    sql_reasoning  = ""
    result: dict = {}
    full_summary = ""
    tokens_used  = 0

    try:
        if upload_table:
            from backend.upload.service import get_upload_schema
            custom_schema = await get_upload_schema(upload_table)
            sql, sql_confidence, sql_reasoning, nl_resp = await natural_language_to_sql(
                query, custom_schema=custom_schema, conversation_history=history or None
            )
        else:
            sql, sql_confidence, sql_reasoning, nl_resp = await natural_language_to_sql(
                query, conversation_history=history or None
            )

        llm_provider = nl_resp.provider
        tokens_used  = nl_resp.tokens_used
        set_sql_cache(query, _SCHEMA_VERSION, sql)
        yield _sse({
            "type": "sql", "sql": sql, "provider": nl_resp.provider,
            "confidence": sql_confidence, "reasoning": sql_reasoning,
        })

        # ── 6. Execute DuckDB query ───────────────────────────────────────
        yield _sse({"type": "status", "message": "Running query…"})
        result = await execute_query(sql)
        result["rows"] = mask_pii_in_results(result["rows"])
        set_result_cache(sql, result)
        yield _sse({
            "type":      "result",
            "columns":   result["columns"],
            "rows":      result["rows"],
            "row_count": result["row_count"],
        })

        # ── 7. Stream summary ─────────────────────────────────────────────
        yield _sse({"type": "status", "message": "Analyzing results…"})
        async for token in summarize_result_stream(query, result):
            full_summary += token
            yield _sse({"type": "token", "content": token})

        set_summary_cache(sql, query, full_summary)

    except (ValueError, RuntimeError) as exc:
        logger.warning("Stream pipeline error for %r: %s", query[:60], exc)
        friendly = (
            "I wasn't able to generate a valid query for that. "
            "Could you rephrase or be more specific? "
            "Mention the metric (revenue, orders) and any filters (region, date)."
        )
        for word in friendly.split():
            yield _sse({"type": "token", "content": word + " "})
            await asyncio.sleep(0.012)

        session  = await _get_or_create_session(db, user, session_id)
        await _save_user_message(db, session.id, query)
        asst_msg = await _save_assistant_message(
            db, session.id, friendly, "", {"columns": [], "row_count": 0}, 0, False, llm_provider,
        )
        await _update_session_title(db, session, query)
        await db.commit()
        yield _sse({
            "type": "done",
            "session_id": session.id, "message_id": asst_msg.id,
            "tokens_used": 0, "cache_hit": False, "llm_provider": llm_provider,
            "latency_ms": round((time.perf_counter() - t_start) * 1000, 2),
            "key_insights": [], "follow_up_questions": _OFF_DOMAIN_SUGGESTIONS,
            "columns": [], "rows": [], "row_count": 0,
        })
        return

    # ── 8. Persist to SQLite ──────────────────────────────────────────────
    total_ms = (time.perf_counter() - t_start) * 1000
    session  = await _get_or_create_session(db, user, session_id)
    await _save_user_message(db, session.id, query)
    asst_msg = await _save_assistant_message(
        db, session.id, full_summary, sql,
        {"columns": result.get("columns", []), "row_count": result.get("row_count", 0)},
        tokens_used, False, llm_provider,
    )
    await _update_session_title(db, session, query)
    await db.commit()

    # ── 9. Metrics + token deduction ─────────────────────────────────────
    await log_metrics(
        db, message_id=asst_msg.id, user_id=user.id,
        latency_ms=total_ms, llm_latency_ms=0.0,
        sql_latency_ms=result.get("sql_latency_ms", 0.0),
        tokens_used=tokens_used, cache_hit=False,
        llm_provider=llm_provider, sql_valid=True,
    )
    if tokens_used > 0:
        await deduct_tokens(db, user, tokens_used)

    yield _sse({
        "type": "done",
        "session_id":  session.id,
        "message_id":  asst_msg.id,
        "tokens_used": tokens_used,
        "cache_hit":   False,
        "llm_provider": llm_provider,
        "latency_ms":  round(total_ms, 2),
        "key_insights": [],
        "follow_up_questions": [],
        "columns":   result.get("columns", []),
        "rows":      result.get("rows", []),
        "row_count": result.get("row_count", 0),
    })
