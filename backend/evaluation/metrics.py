"""
Async helper to persist per-query performance metrics to SQLite.
Called from the chat service after each pipeline run.
"""
import logging
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database.sqlite_manager import QueryMetrics

logger = logging.getLogger(__name__)


async def log_metrics(
    db: AsyncSession,
    *,
    message_id: str,
    user_id: str,
    latency_ms: float,
    llm_latency_ms: float,
    sql_latency_ms: float,
    tokens_used: int,
    cache_hit: bool,
    llm_provider: str,
    sql_valid: bool,
) -> None:
    try:
        metric = QueryMetrics(
            message_id=message_id,
            user_id=user_id,
            latency_ms=round(latency_ms, 2),
            llm_latency_ms=round(llm_latency_ms, 2),
            sql_latency_ms=round(sql_latency_ms, 2),
            tokens_used=tokens_used,
            cache_hit=cache_hit,
            llm_provider=llm_provider,
            sql_valid=sql_valid,
        )
        db.add(metric)
        await db.commit()
    except Exception as exc:
        logger.warning("Failed to log metrics: %s", exc)
