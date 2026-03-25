"""
CSV upload service — parses CSV, loads into DuckDB, stores metadata in SQLite.
"""
import asyncio
import io
import json
import logging
import re
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from fastapi import UploadFile
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database.duckdb_manager import _get_connection, _lock
from backend.database.sqlite_manager import UploadedFile, User

logger = logging.getLogger(__name__)

MAX_FILE_SIZE = 10 * 1024 * 1024   # 10 MB
MAX_ROWS      = 50_000
_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="csv_upload")


def _clean_col(name: str, idx: int) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", str(name)).strip("_")
    return cleaned if cleaned else f"col_{idx}"


def _load_into_duckdb(table_name: str, df: pd.DataFrame) -> None:
    conn = _get_connection()
    with _lock:
        conn.register("_csv_upload_tmp", df)
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM _csv_upload_tmp")
        conn.unregister("_csv_upload_tmp")
    logger.info("Loaded CSV into DuckDB table %s (%d rows)", table_name, len(df))


async def process_csv_upload(
    db: AsyncSession,
    user: User,
    file: UploadFile,
    session_id: str | None = None,
) -> dict:
    content = await file.read()
    if len(content) > MAX_FILE_SIZE:
        raise ValueError("File too large. Maximum allowed size is 10 MB.")

    try:
        df = pd.read_csv(io.StringIO(content.decode("utf-8-sig")))
    except Exception as exc:
        raise ValueError(f"Could not parse CSV: {exc}") from exc

    if df.empty or len(df.columns) == 0:
        raise ValueError("CSV file is empty or has no columns.")

    if len(df) > MAX_ROWS:
        df = df.head(MAX_ROWS)

    # Sanitise column names
    df.columns = [_clean_col(c, i) for i, c in enumerate(df.columns)]

    # Unique table name scoped to user
    stem  = re.sub(r"[^a-zA-Z0-9]", "_", Path(file.filename or "upload").stem)[:24]
    table = f"csv_{stem}_{user.id[:6]}_{uuid.uuid4().hex[:6]}"

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(_executor, _load_into_duckdb, table, df)

    record = UploadedFile(
        id=str(uuid.uuid4()),
        user_id=user.id,
        session_id=session_id,
        table_name=table,
        original_filename=file.filename or "upload.csv",
        row_count=len(df),
        column_names=json.dumps(df.columns.tolist()),
    )
    db.add(record)
    await db.commit()

    return {
        "upload_id":        record.id,
        "table_name":       table,
        "original_filename": file.filename,
        "columns":          df.columns.tolist(),
        "row_count":        len(df),
        "preview":          df.head(5).to_dict(orient="records"),
    }


async def get_upload_schema(table_name: str) -> dict:
    """Return {table_name: [{name, type}]} for a user-uploaded table."""
    def _schema():
        conn = _get_connection()
        with _lock:
            cols = conn.execute(
                "SELECT column_name, data_type "
                "FROM information_schema.columns "
                f"WHERE table_name = '{table_name}' "
                "ORDER BY ordinal_position"
            ).df()
        return [{"name": r["column_name"], "type": r["data_type"]} for _, r in cols.iterrows()]

    loop = asyncio.get_running_loop()
    cols = await loop.run_in_executor(_executor, _schema)
    return {table_name: cols}


def _drop_duckdb_table(table_name: str) -> None:
    """Drop a CSV table from DuckDB. Safe to call even if the table no longer exists."""
    # Only allow dropping tables that start with 'csv_' to prevent accidental deletion
    if not table_name.startswith("csv_"):
        logger.warning("Refused to drop non-CSV table: %s", table_name)
        return
    conn = _get_connection()
    with _lock:
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    logger.info("Dropped DuckDB table %s", table_name)


async def drop_upload(db: AsyncSession, user: User, upload_id: str) -> bool:
    """
    Delete an uploaded CSV — drops the DuckDB table and removes the SQLite record.
    Returns True if found and deleted, False if not found.
    """
    result = await db.execute(
        select(UploadedFile).where(
            UploadedFile.id == upload_id,
            UploadedFile.user_id == user.id,
        )
    )
    record = result.scalar_one_or_none()
    if not record:
        return False

    # Drop from DuckDB first (best-effort — don't fail if already gone)
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(_executor, _drop_duckdb_table, record.table_name)
    except Exception as exc:
        logger.warning("Could not drop DuckDB table %s: %s", record.table_name, exc)

    # Remove SQLite record
    await db.delete(record)
    await db.commit()
    return True


async def list_user_uploads(db: AsyncSession, user: User) -> list[dict]:
    result = await db.execute(
        select(UploadedFile)
        .where(UploadedFile.user_id == user.id)
        .order_by(UploadedFile.created_at.desc())
    )
    rows = result.scalars().all()
    return [
        {
            "upload_id":         r.id,
            "table_name":        r.table_name,
            "original_filename": r.original_filename,
            "columns":           json.loads(r.column_names),
            "row_count":         r.row_count,
            "created_at":        r.created_at,
        }
        for r in rows
    ]
