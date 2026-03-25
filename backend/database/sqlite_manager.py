"""
SQLite manager — stores users, chat sessions, messages, token ledger, query metrics.
Uses SQLAlchemy async engine with aiosqlite.
"""
from datetime import datetime, timezone
from sqlalchemy import (
    Column, String, Integer, BigInteger, Float, Boolean,
    DateTime, Text, ForeignKey, UniqueConstraint, Index, event
)
from sqlalchemy.orm import DeclarativeBase, relationship
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from backend.config import get_settings


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class User(Base):
    __tablename__ = "users"

    id = Column(String(36), primary_key=True)          # UUID
    username = Column(String(64), unique=True, nullable=False, index=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    hashed_password = Column(String(255), nullable=False)
    tier = Column(String(16), nullable=False, default="free")  # free|basic|pro|admin
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))

    sessions = relationship("ChatSession", back_populates="user", cascade="all, delete-orphan")
    token_ledger = relationship("TokenLedger", back_populates="user", cascade="all, delete-orphan")
    refresh_tokens = relationship("RefreshToken", back_populates="user", cascade="all, delete-orphan")
    uploaded_files = relationship("UploadedFile", back_populates="user", cascade="all, delete-orphan")


class RefreshToken(Base):
    __tablename__ = "refresh_tokens"

    id = Column(String(36), primary_key=True)
    user_id = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    token_hash = Column(String(255), nullable=False, unique=True)
    expires_at = Column(DateTime, nullable=False)
    revoked = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))

    user = relationship("User", back_populates="refresh_tokens")


class TokenLedger(Base):
    """Monthly token usage per user."""
    __tablename__ = "token_ledger"
    __table_args__ = (UniqueConstraint("user_id", "year_month", name="uq_user_month"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    year_month = Column(String(7), nullable=False)   # e.g. "2025-03"
    tokens_used = Column(BigInteger, nullable=False, default=0)
    token_limit = Column(BigInteger, nullable=False)

    user = relationship("User", back_populates="token_ledger")


class ChatSession(Base):
    __tablename__ = "chat_sessions"

    id = Column(String(36), primary_key=True)
    user_id = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    title = Column(String(255), nullable=False, default="New Chat")
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))

    user = relationship("User", back_populates="sessions")
    messages = relationship("ChatMessage", back_populates="session", cascade="all, delete-orphan", order_by="ChatMessage.created_at")


class ChatMessage(Base):
    __tablename__ = "chat_messages"

    id = Column(String(36), primary_key=True)
    session_id = Column(String(36), ForeignKey("chat_sessions.id", ondelete="CASCADE"), nullable=False, index=True)
    role = Column(String(16), nullable=False)          # "user" | "assistant"
    content = Column(Text, nullable=False)             # NL query or summary
    generated_sql = Column(Text, nullable=True)        # SQL produced by LLM
    result_json = Column(Text, nullable=True)          # JSON-serialised DataFrame rows
    tokens_used = Column(Integer, nullable=True)
    cache_hit = Column(Boolean, nullable=False, default=False)
    llm_provider = Column(String(16), nullable=True)   # "groq" | "openai"
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))

    session = relationship("ChatSession", back_populates="messages")
    metrics = relationship("QueryMetrics", back_populates="message", uselist=False, cascade="all, delete-orphan")


class QueryMetrics(Base):
    __tablename__ = "query_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(String(36), ForeignKey("chat_messages.id", ondelete="CASCADE"), nullable=False, unique=True)
    user_id = Column(String(36), nullable=False, index=True)
    latency_ms = Column(Float, nullable=True)
    llm_latency_ms = Column(Float, nullable=True)
    sql_latency_ms = Column(Float, nullable=True)
    tokens_used = Column(Integer, nullable=True)
    cache_hit = Column(Boolean, nullable=False, default=False)
    llm_provider = Column(String(16), nullable=True)
    sql_valid = Column(Boolean, nullable=True)
    timestamp = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc), index=True)

    message = relationship("ChatMessage", back_populates="metrics")


class UploadedFile(Base):
    __tablename__ = "uploaded_files"

    id              = Column(String(36), primary_key=True)
    user_id         = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    session_id      = Column(String(36), nullable=True, index=True)
    table_name      = Column(String(255), nullable=False, unique=True)
    original_filename = Column(String(255), nullable=False)
    row_count       = Column(Integer, nullable=False, default=0)
    column_names    = Column(Text, nullable=False)      # JSON array of column names
    created_at      = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))

    user = relationship("User", back_populates="uploaded_files")


# ---------------------------------------------------------------------------
# Engine & session factory
# ---------------------------------------------------------------------------

_engine = None
_session_factory = None


def _get_engine():
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_async_engine(
            settings.sqlite_url,
            echo=settings.debug,
            pool_pre_ping=True,
        )
        # Enable WAL mode for better concurrent read performance
        @event.listens_for(_engine.sync_engine, "connect")
        def set_wal(dbapi_conn, _):
            dbapi_conn.execute("PRAGMA journal_mode=WAL")
            dbapi_conn.execute("PRAGMA foreign_keys=ON")
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            bind=_get_engine(),
            expire_on_commit=False,
            class_=AsyncSession,
        )
    return _session_factory


async def init_db() -> None:
    """Create all tables on startup."""
    engine = _get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_db() -> AsyncSession:
    """FastAPI dependency — yields an async DB session."""
    factory = get_session_factory()
    async with factory() as session:
        yield session
