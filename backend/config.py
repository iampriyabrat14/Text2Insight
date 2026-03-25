from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # LLM
    groq_api_key: str = ""
    openai_api_key: str = ""
    groq_model: str = "llama-3.3-70b-versatile"
    openai_fallback_model: str = "gpt-4o-mini"
    llm_timeout_seconds: int = 8
    circuit_breaker_threshold: int = 3
    circuit_breaker_reset_seconds: int = 60

    # Auth
    jwt_secret_key: str = "change-me-to-a-long-random-string"
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 15
    refresh_token_expire_days: int = 7

    # Token Quotas
    free_tier_limit: int = 10_000
    basic_tier_limit: int = 50_000
    pro_tier_limit: int = 200_000

    # Database
    sqlite_url: str = "sqlite+aiosqlite:///./app.db"
    duckdb_path: str = "./data/sales.duckdb"

    # Cache TTLs (seconds)
    cache_ttl_schema: int = 1800
    cache_ttl_result: int = 300
    cache_ttl_sql: int = 600
    redis_url: str = ""

    # Rate Limiting
    rate_limit_per_minute: int = 20
    max_result_rows: int = 500
    max_query_length: int = 1000

    # Export
    export_temp_dir: str = "./exports"
    export_cleanup_minutes: int = 10

    # App
    debug: bool = False
    log_level: str = "INFO"
    cors_origins: str = "http://localhost:3000,http://127.0.0.1:5500"

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",")]

    def token_limit_for_tier(self, tier: str) -> int:
        return {
            "free": self.free_tier_limit,
            "basic": self.basic_tier_limit,
            "pro": self.pro_tier_limit,
            "admin": 999_999_999,
        }.get(tier, self.free_tier_limit)


@lru_cache
def get_settings() -> Settings:
    return Settings()
