"""
DuckDB manager — query execution and schema introspection.

DuckDB is not async-native, so all blocking calls are wrapped in
asyncio.run_in_executor with a shared ThreadPoolExecutor.
A threading.Lock guards the shared connection for concurrent reads.
"""
import asyncio
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from backend.config import get_settings

logger = logging.getLogger(__name__)

_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="duckdb")
_lock = threading.Lock()
_conn: duckdb.DuckDBPyConnection | None = None


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def _get_connection() -> duckdb.DuckDBPyConnection:
    global _conn
    if _conn is None:
        settings = get_settings()
        path = Path(settings.duckdb_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        _conn = duckdb.connect(str(path))
        logger.info("DuckDB connected: %s", path)
    return _conn


def init_duckdb() -> None:
    """Open the connection at startup (called from lifespan)."""
    _get_connection()


def close_duckdb() -> None:
    global _conn
    if _conn is not None:
        _conn.close()
        _conn = None
        logger.info("DuckDB connection closed")


# ---------------------------------------------------------------------------
# Sync helpers (run inside executor)
# ---------------------------------------------------------------------------

def _execute_query_sync(sql: str, max_rows: int) -> dict[str, Any]:
    """Execute a SELECT and return rows + column names. Thread-safe."""
    conn = _get_connection()
    t0 = time.perf_counter()
    with _lock:
        try:
            rel = conn.execute(sql)
            columns = [desc[0] for desc in rel.description]
            rows = rel.fetchmany(max_rows)
        except duckdb.Error as exc:
            raise ValueError(f"DuckDB execution error: {exc}") from exc
    elapsed_ms = (time.perf_counter() - t0) * 1000

    records = [dict(zip(columns, row)) for row in rows]
    return {
        "columns": columns,
        "rows": records,
        "row_count": len(records),
        "sql_latency_ms": round(elapsed_ms, 2),
    }


def _get_schema_sync() -> dict[str, list[dict[str, str]]]:
    """Return {table_name: [{name, type}, ...]} for all user tables."""
    conn = _get_connection()
    with _lock:
        tables_df: pd.DataFrame = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).df()

    schema: dict[str, list[dict[str, str]]] = {}
    for table_name in tables_df["table_name"].tolist():
        with _lock:
            cols_df: pd.DataFrame = conn.execute(
                f"SELECT column_name, data_type FROM information_schema.columns "
                f"WHERE table_name = '{table_name}' ORDER BY ordinal_position"
            ).df()
        schema[table_name] = [
            {"name": row["column_name"], "type": row["data_type"]}
            for _, row in cols_df.iterrows()
        ]
    return schema


def _get_table_sample_sync(table_name: str, n: int = 3) -> list[dict]:
    """Return n sample rows from a table for prompt context."""
    conn = _get_connection()
    with _lock:
        df = conn.execute(f"SELECT * FROM {table_name} LIMIT {n}").df()
    return df.to_dict(orient="records")


# ---------------------------------------------------------------------------
# Async public API
# ---------------------------------------------------------------------------

async def execute_query(sql: str, max_rows: int | None = None) -> dict[str, Any]:
    """Execute a SELECT query and return results dict."""
    settings = get_settings()
    limit = max_rows or settings.max_result_rows
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _execute_query_sync, sql, limit)


async def get_schema() -> dict[str, list[dict[str, str]]]:
    """Return full schema as {table: [{name, type}]}."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _get_schema_sync)


async def get_schema_as_text() -> str:
    """Return schema formatted for LLM prompt injection."""
    schema = await get_schema()
    lines = []
    for table, cols in schema.items():
        col_defs = ", ".join(f"{c['name']} ({c['type']})" for c in cols)
        lines.append(f"  {table}({col_defs})")
    return "Tables:\n" + "\n".join(lines)


async def get_schema_as_json() -> str:
    """Return schema as JSON string for LLM prompt injection."""
    schema = await get_schema()
    return json.dumps(schema, indent=2)


async def get_table_sample(table_name: str, n: int = 3) -> list[dict]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _get_table_sample_sync, table_name, n)


def result_to_markdown(result: dict[str, Any], max_rows: int = 20) -> str:
    """Convert execute_query result dict to a markdown table for summarization."""
    columns = result["columns"]
    rows = result["rows"][:max_rows]

    if not rows:
        return "_No results found._"

    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    body_lines = [
        "| " + " | ".join(str(row.get(c, "")) for c in columns) + " |"
        for row in rows
    ]
    suffix = f"\n_Showing {len(rows)} of {result['row_count']} rows_" if result["row_count"] > max_rows else ""
    return "\n".join([header, separator] + body_lines) + suffix
