"""
NL → SQL conversion using LLM with schema injection and few-shot examples.

The LLM is asked to return a JSON object:
  {"sql": "...", "confidence": 0.87, "reasoning": "..."}

confidence : float 0.0–1.0 — the model's self-assessed certainty.
reasoning  : 1–2 sentences explaining which tables/columns were used and why.
"""
import json
import logging

from backend.database.duckdb_manager import get_schema_as_json
from backend.llm.client import LLMResponse, llm_chat
from backend.llm.guardrails import extract_sql, validate_sql

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Few-shot examples (DuckDB dialect)
# ---------------------------------------------------------------------------

_FEW_SHOT = """
Examples (respond with JSON as shown):
Q: What is the total revenue by region?
A: {"sql": "SELECT region, ROUND(SUM(oi.sale_price), 2) AS total_revenue FROM orders o JOIN order_items oi ON o.order_id = oi.order_id WHERE o.status = 'Completed' GROUP BY region ORDER BY total_revenue DESC", "confidence": 0.95, "reasoning": "Joined orders and order_items on order_id; filtered to Completed status; grouped by region and summed sale_price."}

Q: Who are the top 5 customers by spend?
A: {"sql": "SELECT c.name, c.segment, ROUND(SUM(oi.sale_price), 2) AS total_spend FROM customers c JOIN orders o ON c.customer_id = o.customer_id JOIN order_items oi ON o.order_id = oi.order_id WHERE o.status = 'Completed' GROUP BY c.customer_id, c.name, c.segment ORDER BY total_spend DESC LIMIT 5", "confidence": 0.97, "reasoning": "Three-table join through orders; limited to 5 rows and sorted descending by total spend."}

Q: Which product categories have the highest profit margin?
A: {"sql": "SELECT p.category, ROUND(AVG((oi.sale_price / oi.quantity - p.cost) / NULLIF(p.cost, 0) * 100), 2) AS avg_margin_pct FROM order_items oi JOIN products p ON oi.product_id = p.product_id JOIN orders o ON oi.order_id = o.order_id WHERE o.status = 'Completed' GROUP BY p.category ORDER BY avg_margin_pct DESC", "confidence": 0.91, "reasoning": "Used NULLIF to prevent division by zero when cost is 0; computed per-unit margin from sale_price / quantity minus cost."}
"""

_SYSTEM_PROMPT = """\
You are an expert DuckDB SQL analyst.
Given the database schema and a natural language question, return a single JSON object.

Output ONLY valid JSON — no markdown, no explanation outside the JSON:
{{
  "sql":        "<single valid DuckDB SELECT query>",
  "confidence": <float 0.0–1.0, your certainty this SQL answers the question>,
  "reasoning":  "<1–2 sentences: which tables/joins/filters you chose and why>"
}}

Rules for the SQL field:
- Use only SELECT statements. Never use INSERT, UPDATE, DELETE, DROP, or any DDL.
- Use table aliases for clarity.
- Cast date strings with CAST(column AS DATE) when doing date arithmetic.
- Use NULLIF to avoid division by zero.
- Default LIMIT to 100 rows unless the user specifies otherwise.
- For currency/percentages use ROUND(..., 2).
{context_rule}
Schema:
{schema}

{few_shot}
"""

_CONTEXT_RULE = (
    "- The conversation history above provides context for follow-up questions. "
    "Resolve references like \"that\", \"those results\", \"add X to that\", "
    "\"filter by\", \"now show\" by extending or modifying the previous SQL query.\n"
)


def _parse_nl_response(raw: str) -> tuple[str, float, str]:
    """
    Parse the LLM's JSON response.
    Returns (sql, confidence, reasoning).
    Falls back gracefully if JSON is malformed.
    """
    try:
        obj = json.loads(raw)
        sql = str(obj.get("sql", "")).strip()
        confidence = float(obj.get("confidence", 0.5))
        confidence = max(0.0, min(1.0, confidence))   # clamp to [0, 1]
        reasoning = str(obj.get("reasoning", "")).strip()
        return sql, confidence, reasoning
    except (json.JSONDecodeError, TypeError, ValueError):
        # JSON mode failed — treat the whole response as raw SQL (legacy fallback)
        logger.warning("NL→SQL JSON parse failed; falling back to raw SQL extraction")
        return extract_sql(raw), 0.5, ""


async def natural_language_to_sql(
    user_query: str,
    custom_schema: dict | None = None,
    conversation_history: list[dict] | None = None,
) -> tuple[str, float, str, LLMResponse]:
    """
    Convert a natural language query to SQL.
    Returns (validated_sql, confidence, reasoning, llm_response).
    Raises ValueError if the LLM output fails guardrails.

    - custom_schema: use instead of the full DuckDB schema (for CSV uploads).
    - conversation_history: list of {"role", "content"} dicts for prior turns.
    """
    if custom_schema:
        schema_json = json.dumps(custom_schema, indent=2)
    else:
        schema_json = await get_schema_as_json()

    has_history  = bool(conversation_history)
    context_rule = _CONTEXT_RULE if has_history else ""
    system = _SYSTEM_PROMPT.format(
        schema=schema_json,
        few_shot=_FEW_SHOT if not custom_schema else "",
        context_rule=context_rule,
    )

    messages: list[dict] = [{"role": "system", "content": system}]
    if has_history:
        messages.extend(conversation_history)
    messages.append({"role": "user", "content": user_query})

    llm_resp = await llm_chat(messages, temperature=0.0, max_tokens=700, json_mode=True)
    raw = llm_resp.content

    sql, confidence, reasoning = _parse_nl_response(raw)

    # If SQL was empty after parsing, try extracting from raw as last resort
    if not sql:
        sql = extract_sql(raw)

    guard = validate_sql(sql)
    if not guard.passed:
        raise ValueError(f"Generated SQL failed validation: {guard.reason}\nSQL: {sql}")

    logger.info(
        "NL→SQL [%s, %.0fms, conf=%.2f]: %s",
        llm_resp.provider, llm_resp.latency_ms, confidence, sql[:120],
    )
    return sql, confidence, reasoning, llm_resp
