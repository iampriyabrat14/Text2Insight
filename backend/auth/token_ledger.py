"""
Token quota management — tracks per-user monthly token usage in SQLite.
"""
from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import get_settings
from backend.database.sqlite_manager import TokenLedger, User


def _current_year_month() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


async def get_or_create_ledger(db: AsyncSession, user: User) -> TokenLedger:
    """Get current month's ledger row, creating it if needed."""
    year_month = _current_year_month()
    result = await db.execute(
        select(TokenLedger).where(
            TokenLedger.user_id == user.id,
            TokenLedger.year_month == year_month,
        )
    )
    ledger = result.scalar_one_or_none()
    if ledger is None:
        settings = get_settings()
        limit = settings.token_limit_for_tier(user.tier)
        ledger = TokenLedger(
            user_id=user.id,
            year_month=year_month,
            tokens_used=0,
            token_limit=limit,
        )
        db.add(ledger)
        await db.commit()
        await db.refresh(ledger)
    return ledger


async def check_quota(db: AsyncSession, user: User) -> tuple[bool, TokenLedger]:
    """Returns (has_quota, ledger). Admins always have quota."""
    if user.tier == "admin":
        ledger = await get_or_create_ledger(db, user)
        return True, ledger
    ledger = await get_or_create_ledger(db, user)
    return ledger.tokens_used < ledger.token_limit, ledger


async def deduct_tokens(db: AsyncSession, user: User, tokens: int) -> TokenLedger:
    """Deduct tokens from the monthly quota. Returns updated ledger."""
    if tokens <= 0:
        return await get_or_create_ledger(db, user)
    ledger = await get_or_create_ledger(db, user)
    ledger.tokens_used += tokens
    await db.commit()
    await db.refresh(ledger)
    return ledger


async def get_quota_info(db: AsyncSession, user: User) -> dict:
    ledger = await get_or_create_ledger(db, user)
    remaining = max(0, ledger.token_limit - ledger.tokens_used)
    percent = round((ledger.tokens_used / ledger.token_limit) * 100, 1) if ledger.token_limit else 0
    return {
        "tokens_used": ledger.tokens_used,
        "token_limit": ledger.token_limit,
        "year_month": ledger.year_month,
        "remaining": remaining,
        "percent_used": percent,
    }
