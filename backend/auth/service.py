"""
Auth service — password hashing, JWT creation/validation, refresh token management.
"""
import hashlib
import logging
import uuid
from datetime import datetime, timedelta, timezone

import bcrypt as _bcrypt
from jose import JWTError, jwt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import get_settings
from backend.database.sqlite_manager import RefreshToken, User

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Password helpers
# ---------------------------------------------------------------------------

def _prepare_password(plain: str) -> bytes:
    """SHA-256 pre-hash → fixed 64-char hex, avoids bcrypt 72-byte limit."""
    return hashlib.sha256(plain.encode()).hexdigest().encode()


def hash_password(plain: str) -> str:
    return _bcrypt.hashpw(_prepare_password(plain), _bcrypt.gensalt(rounds=12)).decode()


def verify_password(plain: str, hashed: str) -> bool:
    return _bcrypt.checkpw(_prepare_password(plain), hashed.encode())


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(timezone.utc)


def create_access_token(user_id: str, username: str, tier: str) -> tuple[str, int]:
    """Returns (token, expires_in_seconds)."""
    settings = get_settings()
    expires_in = settings.access_token_expire_minutes * 60
    payload = {
        "sub": user_id,
        "username": username,
        "tier": tier,
        "type": "access",
        "exp": _now() + timedelta(seconds=expires_in),
        "iat": _now(),
        "jti": str(uuid.uuid4()),
    }
    token = jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)
    return token, expires_in


def create_refresh_token_value() -> str:
    """Generate a random opaque refresh token value."""
    return str(uuid.uuid4()) + "-" + str(uuid.uuid4())


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def decode_access_token(token: str) -> dict:
    """Decode and validate access token. Raises JWTError on failure."""
    settings = get_settings()
    payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
    if payload.get("type") != "access":
        raise JWTError("Not an access token")
    return payload


# ---------------------------------------------------------------------------
# User CRUD
# ---------------------------------------------------------------------------

async def create_user(db: AsyncSession, username: str, email: str, password: str, tier: str = "free") -> User:
    # Check duplicates
    existing = await db.execute(
        select(User).where((User.username == username) | (User.email == email))
    )
    if existing.scalar_one_or_none():
        raise ValueError("Username or email already registered")

    user = User(
        id=str(uuid.uuid4()),
        username=username,
        email=email,
        hashed_password=hash_password(password),
        tier=tier,
        is_active=True,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return user


async def get_user_by_username(db: AsyncSession, username: str) -> User | None:
    result = await db.execute(select(User).where(User.username == username))
    return result.scalar_one_or_none()


async def get_user_by_id(db: AsyncSession, user_id: str) -> User | None:
    result = await db.execute(select(User).where(User.id == user_id))
    return result.scalar_one_or_none()


async def authenticate_user(db: AsyncSession, username: str, password: str) -> User | None:
    user = await get_user_by_username(db, username)
    if not user or not user.is_active:
        return None
    if not verify_password(password, user.hashed_password):
        return None
    return user


# ---------------------------------------------------------------------------
# Refresh token management
# ---------------------------------------------------------------------------

async def store_refresh_token(db: AsyncSession, user_id: str, token_value: str) -> None:
    settings = get_settings()
    rt = RefreshToken(
        id=str(uuid.uuid4()),
        user_id=user_id,
        token_hash=_hash_token(token_value),
        expires_at=_now() + timedelta(days=settings.refresh_token_expire_days),
        revoked=False,
    )
    db.add(rt)
    await db.commit()


async def validate_and_rotate_refresh_token(
    db: AsyncSession, token_value: str
) -> User | None:
    """
    Validates refresh token, marks it revoked, issues nothing (caller issues new one).
    Returns the User if valid, None otherwise.
    """
    token_hash = _hash_token(token_value)
    result = await db.execute(
        select(RefreshToken).where(
            RefreshToken.token_hash == token_hash,
            RefreshToken.revoked == False,  # noqa: E712
        )
    )
    rt: RefreshToken | None = result.scalar_one_or_none()

    if rt is None:
        return None
    if rt.expires_at.replace(tzinfo=timezone.utc) < _now():
        return None

    # Revoke the used token (rotation)
    rt.revoked = True
    await db.commit()

    return await get_user_by_id(db, rt.user_id)


async def revoke_all_refresh_tokens(db: AsyncSession, user_id: str) -> None:
    """Logout — invalidate all refresh tokens for the user."""
    result = await db.execute(
        select(RefreshToken).where(
            RefreshToken.user_id == user_id,
            RefreshToken.revoked == False,  # noqa: E712
        )
    )
    for rt in result.scalars().all():
        rt.revoked = True
    await db.commit()
