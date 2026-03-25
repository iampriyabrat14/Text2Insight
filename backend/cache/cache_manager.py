"""
Cache manager — TTLCache backed in-process store with a Redis-ready interface.

All keys are namespaced:
  schema:<hash>   → DuckDB schema JSON string
  sql:<hash>      → generated SQL string
  result:<hash>   → query result dict
  summary:<hash>  → summary string

Cache keys are deterministic MD5 hashes of the input content.
"""
import hashlib
import json
import logging
from typing import Any

from cachetools import TTLCache

from backend.config import get_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-process caches (one per TTL bucket)
# ---------------------------------------------------------------------------

_schema_cache: TTLCache | None = None
_sql_cache: TTLCache | None = None
_result_cache: TTLCache | None = None
_summary_cache: TTLCache | None = None

_stats = {"hits": 0, "misses": 0}


def _caches() -> dict[str, TTLCache]:
    global _schema_cache, _sql_cache, _result_cache, _summary_cache
    if _schema_cache is None:
        s = get_settings()
        _schema_cache = TTLCache(maxsize=4, ttl=s.cache_ttl_schema)
        _sql_cache = TTLCache(maxsize=512, ttl=s.cache_ttl_sql)
        _result_cache = TTLCache(maxsize=256, ttl=s.cache_ttl_result)
        _summary_cache = TTLCache(maxsize=256, ttl=s.cache_ttl_result)
    return {
        "schema": _schema_cache,
        "sql": _sql_cache,
        "result": _result_cache,
        "summary": _summary_cache,
    }


# ---------------------------------------------------------------------------
# Key generation
# ---------------------------------------------------------------------------

def make_key(*parts: str) -> str:
    """Deterministic MD5 key from one or more string parts."""
    combined = "|".join(parts)
    return hashlib.md5(combined.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Generic get / set
# ---------------------------------------------------------------------------

def _namespace(key: str, ns: str) -> str:
    return f"{ns}:{key}"


def cache_get(key: str, namespace: str) -> Any | None:
    cache = _caches()[namespace]
    ns_key = _namespace(key, namespace)
    val = cache.get(ns_key)
    if val is not None:
        _stats["hits"] += 1
        logger.debug("Cache HIT  [%s] %s", namespace, key[:16])
    else:
        _stats["misses"] += 1
        logger.debug("Cache MISS [%s] %s", namespace, key[:16])
    return val


def cache_set(key: str, namespace: str, value: Any) -> None:
    cache = _caches()[namespace]
    ns_key = _namespace(key, namespace)
    cache[ns_key] = value
    logger.debug("Cache SET  [%s] %s", namespace, key[:16])


def cache_stats() -> dict:
    total = _stats["hits"] + _stats["misses"]
    hit_rate = round(_stats["hits"] / total * 100, 1) if total else 0.0
    return {**_stats, "total": total, "hit_rate_pct": hit_rate}


def cache_clear_all() -> None:
    for c in _caches().values():
        c.clear()
    _stats["hits"] = 0
    _stats["misses"] = 0


# ---------------------------------------------------------------------------
# Typed helpers for each namespace
# ---------------------------------------------------------------------------

def get_schema_cache(schema_version: str) -> str | None:
    return cache_get(schema_version, "schema")


def set_schema_cache(schema_version: str, value: str) -> None:
    cache_set(schema_version, "schema", value)


def get_sql_cache(query: str, schema_version: str) -> str | None:
    key = make_key(schema_version, query.strip().lower())
    return cache_get(key, "sql")


def set_sql_cache(query: str, schema_version: str, sql: str) -> None:
    key = make_key(schema_version, query.strip().lower())
    cache_set(key, "sql", sql)


def get_result_cache(sql: str) -> dict | None:
    key = make_key(sql.strip())
    return cache_get(key, "result")


def set_result_cache(sql: str, result: dict) -> None:
    key = make_key(sql.strip())
    cache_set(key, "result", result)


def get_summary_cache(sql: str, user_query: str) -> str | None:
    key = make_key(sql.strip(), user_query.strip().lower())
    return cache_get(key, "summary")


def set_summary_cache(sql: str, user_query: str, summary: str) -> None:
    key = make_key(sql.strip(), user_query.strip().lower())
    cache_set(key, "summary", summary)
