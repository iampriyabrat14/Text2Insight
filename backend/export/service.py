"""
Export service — loads session from SQLite, builds ExportSession, dispatches to exporter.
"""
import asyncio
import logging
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import get_settings
from backend.database.duckdb_manager import execute_query
from backend.database.sqlite_manager import ChatMessage, ChatSession, User
from backend.export.models import ExportMessage, ExportSession
from backend.export.pdf_exporter import generate_pdf
from backend.export.ppt_exporter import generate_ppt
from backend.export.word_exporter import generate_word

logger = logging.getLogger(__name__)

_SUPPORTED_FORMATS = {"pdf", "word", "ppt"}
_MIME = {
    "pdf":  "application/pdf",
    "word": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "ppt":  "application/vnd.openxmlformats-officedocument.presentationml.presentation",
}
_EXT = {"pdf": "pdf", "word": "docx", "ppt": "pptx"}

_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="export")


async def _load_export_session(
    db: AsyncSession, user: User, session_id: str, message_id: str | None = None
) -> ExportSession | None:
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

    if message_id:
        # Find the target assistant message and include the user message before it
        target_idx = next(
            (i for i, m in enumerate(messages) if str(m.id) == str(message_id)),
            None,
        )
        if target_idx is not None:
            pair_start = (
                target_idx - 1
                if target_idx > 0 and messages[target_idx - 1].role == "user"
                else target_idx
            )
            messages = messages[pair_start : target_idx + 1]
        else:
            messages = []

    # Re-execute SQL for assistant messages so we have fresh rows for table/chart
    export_messages = []
    for m in messages:
        result_data = None
        if m.role == "assistant" and m.generated_sql and m.generated_sql.strip():
            try:
                res = await execute_query(m.generated_sql)
                result_data = {
                    "columns": res.get("columns", []),
                    "rows":    res.get("rows", [])[:50],   # cap at 50 rows for export
                }
            except Exception as exc:
                logger.debug("Export: could not re-execute SQL for message %s: %s", m.id, exc)

        export_messages.append(ExportMessage(
            role=m.role,
            content=m.content,
            generated_sql=m.generated_sql,
            result_data=result_data,
            tokens_used=m.tokens_used,
            cache_hit=m.cache_hit,
            llm_provider=m.llm_provider,
            created_at=m.created_at.replace(tzinfo=timezone.utc)
                if m.created_at and m.created_at.tzinfo is None else m.created_at,
        ))

    return ExportSession(
        session_id=session.id,
        title=session.title,
        username=user.username,
        user_tier=user.tier,
        created_at=session.created_at.replace(tzinfo=timezone.utc)
            if session.created_at.tzinfo is None else session.created_at,
        messages=export_messages,
    )


def _generate_sync(fmt: str, export_session: ExportSession) -> bytes:
    if fmt == "pdf":
        return generate_pdf(export_session)
    if fmt == "word":
        return generate_word(export_session)
    if fmt == "ppt":
        return generate_ppt(export_session)
    raise ValueError(f"Unknown format: {fmt}")


async def build_export(
    db: AsyncSession,
    user: User,
    session_id: str,
    fmt: str,
    message_id: str | None = None,
) -> tuple[bytes, str, str] | None:
    """
    Returns (file_bytes, filename, mime_type) or None if session not found.
    Raises ValueError for unsupported format.
    """
    if fmt not in _SUPPORTED_FORMATS:
        raise ValueError(f"Unsupported format '{fmt}'. Choose from: {', '.join(_SUPPORTED_FORMATS)}")

    export_session = await _load_export_session(db, user, session_id, message_id)
    if export_session is None:
        return None

    # Run CPU-bound generation off the event loop
    loop = asyncio.get_running_loop()
    file_bytes = await loop.run_in_executor(_executor, _generate_sync, fmt, export_session)

    safe_title = "".join(c if c.isalnum() or c in "-_ " else "_" for c in export_session.title)[:40]
    filename = f"{safe_title}_{session_id[:8]}.{_EXT[fmt]}"
    mime = _MIME[fmt]

    logger.info(
        "Export: %s — format=%s  size=%d bytes  user=%s",
        session_id[:8], fmt, len(file_bytes), user.username,
    )
    return file_bytes, filename, mime
