"""
Sliding-window rate limiter — per authenticated user, in-process.
Integrates as a FastAPI dependency injected into protected routes.

Swap the _store dict for Redis (ZREMRANGEBYSCORE + ZADD) for multi-instance deploys.
"""
import time
import logging
from collections import defaultdict, deque

from fastapi import Depends, HTTPException, status

from backend.config import get_settings
from backend.database.sqlite_manager import User
from backend.dependencies import get_current_user

logger = logging.getLogger(__name__)

# user_id → deque of request timestamps (float seconds)
_windows: dict[str, deque] = defaultdict(deque)


def _check_rate_limit(user_id: str, limit: int, window_seconds: int = 60) -> None:
    now = time.monotonic()
    cutoff = now - window_seconds
    q = _windows[user_id]

    # Evict old entries
    while q and q[0] < cutoff:
        q.popleft()

    if len(q) >= limit:
        oldest = q[0]
        retry_after = int(window_seconds - (now - oldest)) + 1
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded. Try again in {retry_after}s.",
            headers={"Retry-After": str(retry_after)},
        )

    q.append(now)


def rate_limit(user: User = Depends(get_current_user)) -> User:
    """FastAPI dependency — raises 429 if user exceeds per-minute query limit."""
    settings = get_settings()
    # Admins are exempt
    if user.tier != "admin":
        _check_rate_limit(user.id, limit=settings.rate_limit_per_minute, window_seconds=60)
    return user
