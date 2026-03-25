from datetime import datetime
from typing import Any
from pydantic import BaseModel


class RenameRequest(BaseModel):
    title: str


class QueryRequest(BaseModel):
    query: str
    session_id: str | None = None   # omit to start a new session
    upload_table: str | None = None   # table name of user-uploaded CSV


class QueryResponse(BaseModel):
    session_id: str
    message_id: str
    query: str
    generated_sql: str
    sql_confidence: float = 0.0    # LLM self-assessed certainty, 0.0–1.0
    sql_reasoning: str = ""        # LLM explanation of table/join choices
    row_count: int
    columns: list[str]
    rows: list[dict[str, Any]]
    summary: str
    key_insights: list[str] = []
    follow_up_questions: list[str] = []
    tokens_used: int
    cache_hit: bool
    llm_provider: str
    latency_ms: float


class MessageOut(BaseModel):
    id: str
    role: str
    content: str
    generated_sql: str | None
    row_count: int | None
    tokens_used: int | None
    cache_hit: bool
    llm_provider: str | None
    created_at: datetime


class SessionOut(BaseModel):
    id: str
    title: str
    created_at: datetime
    updated_at: datetime
    message_count: int = 0


class SessionDetail(BaseModel):
    id: str
    title: str
    created_at: datetime
    updated_at: datetime
    messages: list[MessageOut]
