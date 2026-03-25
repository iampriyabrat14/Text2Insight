import pytest
from backend.cache.cache_manager import (
    cache_clear_all, cache_stats,
    get_sql_cache, set_sql_cache,
    get_result_cache, set_result_cache,
    get_summary_cache, set_summary_cache,
)


@pytest.fixture(autouse=True)
def reset_cache():
    cache_clear_all()
    yield
    cache_clear_all()


def test_sql_cache_miss():
    assert get_sql_cache("show revenue", "v1") is None


def test_sql_cache_hit():
    set_sql_cache("show revenue", "v1", "SELECT SUM(sale_price) FROM order_items")
    result = get_sql_cache("show revenue", "v1")
    assert result == "SELECT SUM(sale_price) FROM order_items"


def test_sql_cache_key_includes_schema_version():
    set_sql_cache("show revenue", "v1", "SQL_v1")
    set_sql_cache("show revenue", "v2", "SQL_v2")
    assert get_sql_cache("show revenue", "v1") == "SQL_v1"
    assert get_sql_cache("show revenue", "v2") == "SQL_v2"


def test_result_cache_roundtrip():
    result = {"columns": ["a"], "rows": [{"a": 1}], "row_count": 1, "sql_latency_ms": 1.0}
    sql = "SELECT a FROM t"
    set_result_cache(sql, result)
    assert get_result_cache(sql) == result


def test_summary_cache_roundtrip():
    set_summary_cache("SELECT 1", "show revenue", "Revenue is high")
    assert get_summary_cache("SELECT 1", "show revenue") == "Revenue is high"


def test_cache_stats():
    get_sql_cache("miss", "v1")          # miss
    set_sql_cache("hit_q", "v1", "SQL")
    get_sql_cache("hit_q", "v1")          # hit
    stats = cache_stats()
    assert stats["hits"] >= 1
    assert stats["misses"] >= 1
    assert 0 <= stats["hit_rate_pct"] <= 100
