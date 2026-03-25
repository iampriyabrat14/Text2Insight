"""
Summarizes DuckDB query results in natural language using an LLM.
Returns a business summary + follow-up question suggestions.
"""
import json
import logging

from backend.database.duckdb_manager import result_to_markdown
from backend.llm.client import LLMResponse, llm_chat, llm_stream

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """\
You are a sharp business analyst presenting data insights to an executive audience.

Given the query result, respond with a JSON object in this exact format:
{
  "summary": "<2-4 sentence business summary. Lead with the single most important finding. Include key numbers, top/bottom performers, trends, or anomalies. Use plain English — no SQL or technical terms.>",
  "key_insights": ["<insight 1>", "<insight 2>", "<insight 3>"],
  "follow_up_questions": ["<relevant follow-up question 1>", "<relevant follow-up question 2>", "<relevant follow-up question 3>"]
}

Rules:
- summary: max 100 words, factual, data-driven
- key_insights: exactly 3 short bullet-style facts from the data
- follow_up_questions: exactly 3 natural follow-up questions a business user would ask next
- Return ONLY valid JSON, no markdown fences, no extra text
"""


async def summarize_result(
    user_query: str,
    result: dict,
    max_table_rows: int = 20,
) -> tuple[str, list[str], list[str], LLMResponse]:
    """
    Summarize a query result.
    Returns (summary, key_insights, follow_up_questions, llm_response).
    """
    table_md = result_to_markdown(result, max_rows=max_table_rows)
    row_count = result.get("row_count", 0)

    user_content = (
        f"User question: {user_query}\n\n"
        f"Query result ({row_count} rows):\n{table_md}"
    )

    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]

    llm_resp = await llm_chat(messages, temperature=0.3, max_tokens=512)
    raw = llm_resp.content.strip()

    # Parse JSON; fall back to plain text if LLM doesn't comply
    try:
        parsed = json.loads(raw)
        summary = parsed.get("summary", raw)
        key_insights = parsed.get("key_insights", [])[:3]
        follow_up_questions = parsed.get("follow_up_questions", [])[:3]
    except (json.JSONDecodeError, AttributeError):
        summary = raw
        key_insights = []
        follow_up_questions = []

    logger.info(
        "Summarizer [%s, %.0fms]: %d rows → summary + %d follow-ups",
        llm_resp.provider, llm_resp.latency_ms, row_count, len(follow_up_questions),
    )
    return summary, key_insights, follow_up_questions, llm_resp


# ---------------------------------------------------------------------------
# Streaming summariser — plain-text tokens, no JSON wrapper
# ---------------------------------------------------------------------------

_STREAM_SYSTEM_PROMPT = """\
You are a sharp business analyst. Write a clear 2-4 sentence summary of the query result.
Lead with the single most important finding. Include key numbers and notable trends.
Output ONLY the summary text — no JSON, no markdown, no headers, no extra formatting.
"""


async def summarize_result_stream(
    user_query: str,
    result: dict,
    max_table_rows: int = 20,
):
    """Async generator — yields plain-text summary tokens as they stream from the LLM."""
    table_md = result_to_markdown(result, max_rows=max_table_rows)
    row_count = result.get("row_count", 0)
    messages = [
        {"role": "system", "content": _STREAM_SYSTEM_PROMPT},
        {"role": "user", "content": f"User question: {user_query}\n\nResult ({row_count} rows):\n{table_md}"},
    ]
    async for token in llm_stream(messages, temperature=0.3, max_tokens=300):
        yield token
