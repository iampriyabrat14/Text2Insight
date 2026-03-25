"""
Analytics / evaluation router — admin-only observability endpoints.
Queries query_metrics and chat_messages tables to surface KPIs.
All endpoints require admin tier.
"""
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database.sqlite_manager import ChatMessage, QueryMetrics, User, get_db
from backend.dependencies import require_admin

router = APIRouter()


def _since(days: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days)


# ── Summary KPIs ────────────────────────────────────────────────────────────

@router.get("/summary")
async def get_summary(
    days: int = 30,
    _: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    since = _since(days)

    agg = await db.execute(
        select(
            func.count().label("total"),
            func.avg(QueryMetrics.latency_ms).label("avg_latency"),
            func.avg(QueryMetrics.llm_latency_ms).label("avg_llm_latency"),
            func.sum(QueryMetrics.tokens_used).label("total_tokens"),
        ).where(QueryMetrics.timestamp >= since)
    )
    row = agg.one()

    cache_hits = (await db.execute(
        select(func.count()).where(
            QueryMetrics.timestamp >= since,
            QueryMetrics.cache_hit == True,  # noqa: E712
        )
    )).scalar() or 0

    sql_valid = (await db.execute(
        select(func.count()).where(
            QueryMetrics.timestamp >= since,
            QueryMetrics.sql_valid == True,  # noqa: E712
        )
    )).scalar() or 0

    total = row.total or 0
    return {
        "total_queries":      total,
        "avg_latency_ms":     round(row.avg_latency or 0, 1),
        "avg_llm_latency_ms": round(row.avg_llm_latency or 0, 1),
        "total_tokens":       row.total_tokens or 0,
        "cache_hit_rate":     round(cache_hits / total * 100, 1) if total else 0.0,
        "sql_valid_rate":     round(sql_valid  / total * 100, 1) if total else 0.0,
        "days": days,
    }


# ── Latency percentiles ─────────────────────────────────────────────────────

@router.get("/latency-percentiles")
async def get_latency_percentiles(
    days: int = 30,
    _: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    since = _since(days)
    result = await db.execute(
        select(QueryMetrics.latency_ms)
        .where(
            QueryMetrics.timestamp >= since,
            QueryMetrics.latency_ms.is_not(None),
        )
        .order_by(QueryMetrics.latency_ms)
    )
    values = [r[0] for r in result.all()]
    if not values:
        return {"p50": 0, "p75": 0, "p95": 0, "p99": 0}

    def _p(data, pct):
        idx = max(0, int(len(data) * pct / 100) - 1)
        return round(data[idx], 1)

    return {
        "p50": _p(values, 50),
        "p75": _p(values, 75),
        "p95": _p(values, 95),
        "p99": _p(values, 99),
    }


# ── Provider breakdown ──────────────────────────────────────────────────────

@router.get("/provider-breakdown")
async def get_provider_breakdown(
    days: int = 30,
    _: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    since = _since(days)
    result = await db.execute(
        select(QueryMetrics.llm_provider, func.count().label("count"))
        .where(QueryMetrics.timestamp >= since)
        .group_by(QueryMetrics.llm_provider)
        .order_by(func.count().desc())
    )
    return [{"provider": r.llm_provider or "unknown", "count": r.count}
            for r in result.all()]


# ── Daily query volume ──────────────────────────────────────────────────────

@router.get("/daily-volume")
async def get_daily_volume(
    days: int = 30,
    _: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    since = _since(days)
    result = await db.execute(
        select(
            func.date(QueryMetrics.timestamp).label("day"),
            func.count().label("count"),
        )
        .where(QueryMetrics.timestamp >= since)
        .group_by(func.date(QueryMetrics.timestamp))
        .order_by(func.date(QueryMetrics.timestamp))
    )
    return [{"day": r.day, "count": r.count} for r in result.all()]


# ── Hourly distribution ─────────────────────────────────────────────────────

@router.get("/hourly-distribution")
async def get_hourly_distribution(
    days: int = 30,
    _: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    since = _since(days)
    result = await db.execute(
        select(
            func.strftime("%H", QueryMetrics.timestamp).label("hour"),
            func.count().label("count"),
        )
        .where(QueryMetrics.timestamp >= since)
        .group_by(func.strftime("%H", QueryMetrics.timestamp))
        .order_by(func.strftime("%H", QueryMetrics.timestamp))
    )
    hour_map = {int(r.hour): r.count for r in result.all()}
    return [{"hour": h, "count": hour_map.get(h, 0)} for h in range(24)]


# ── Top queries ─────────────────────────────────────────────────────────────

@router.get("/top-queries")
async def get_top_queries(
    days: int = 30,
    limit: int = 10,
    _: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    since = _since(days)
    result = await db.execute(
        select(
            ChatMessage.content.label("query"),
            func.count().label("count"),
        )
        .where(
            ChatMessage.role == "user",
            ChatMessage.created_at >= since,
        )
        .group_by(ChatMessage.content)
        .order_by(func.count().desc())
        .limit(limit)
    )
    return [{"query": r.query[:120], "count": r.count} for r in result.all()]


# ── User activity ───────────────────────────────────────────────────────────

@router.get("/user-stats")
async def get_user_stats(
    days: int = 30,
    limit: int = 10,
    _: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    since = _since(days)
    result = await db.execute(
        select(
            User.username,
            User.tier,
            func.count(QueryMetrics.id).label("query_count"),
            func.coalesce(func.sum(QueryMetrics.tokens_used), 0).label("tokens_used"),
            func.avg(QueryMetrics.latency_ms).label("avg_latency"),
        )
        .join(User, QueryMetrics.user_id == User.id)
        .where(QueryMetrics.timestamp >= since)
        .group_by(QueryMetrics.user_id, User.username, User.tier)
        .order_by(func.count(QueryMetrics.id).desc())
        .limit(limit)
    )
    return [
        {
            "username":      r.username,
            "tier":          r.tier,
            "query_count":   r.query_count,
            "tokens_used":   r.tokens_used or 0,
            "avg_latency_ms": round(r.avg_latency or 0, 1),
        }
        for r in result.all()
    ]
