"""
Shared data structures for export — decoupled from SQLAlchemy models.
"""
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class ExportMessage:
    role: str                          # "user" | "assistant"
    content: str                       # query text or summary
    generated_sql: str | None = None
    result_data: dict | None = None    # {"columns": [...], "rows": [...]} re-fetched at export time
    tokens_used: int | None = None
    cache_hit: bool = False
    llm_provider: str | None = None
    created_at: datetime | None = None


@dataclass
class ExportSession:
    session_id: str
    title: str
    username: str
    user_tier: str
    created_at: datetime
    messages: list[ExportMessage] = field(default_factory=list)

    # ── Derived helpers ──────────────────────────────────────────────────

    @property
    def qa_pairs(self) -> list[dict]:
        """Return list of {query, sql, summary, tokens, provider, created_at} dicts."""
        pairs = []
        msgs = self.messages
        i = 0
        while i < len(msgs):
            if msgs[i].role == "user" and i + 1 < len(msgs) and msgs[i + 1].role == "assistant":
                user_msg = msgs[i]
                asst_msg = msgs[i + 1]
                pairs.append({
                    "query":       user_msg.content,
                    "sql":         asst_msg.generated_sql or "",
                    "summary":     asst_msg.content,
                    "result_data": asst_msg.result_data,   # dict | None
                    "tokens":      asst_msg.tokens_used or 0,
                    "provider":    asst_msg.llm_provider or "—",
                    "cache_hit":   asst_msg.cache_hit,
                    "created_at":  asst_msg.created_at,
                })
                i += 2
            else:
                i += 1
        return pairs

    @property
    def total_tokens(self) -> int:
        return sum(m.tokens_used or 0 for m in self.messages if m.role == "assistant")
