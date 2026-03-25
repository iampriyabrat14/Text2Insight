"""
Guardrails — input validation and SQL safety checks.

Input guardrails  : applied to the user's natural language query BEFORE LLM call.
SQL guardrails    : applied to LLM-generated SQL BEFORE DuckDB execution.
Output guardrails : applied to result rows BEFORE returning to the client.
"""
import re
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Patterns
# ---------------------------------------------------------------------------

# SQL-modifying keywords that must never appear in generated queries
_FORBIDDEN_SQL = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|EXEC|EXECUTE|MERGE|REPLACE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)

# Prompt injection attempts
_INJECTION_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"ignore\s+(all\s+)?previous\s+instructions",
        r"disregard\s+(all\s+)?previous",
        r"forget\s+(all\s+)?previous",
        r"you\s+are\s+now\s+a",
        r"act\s+as\s+(if\s+you\s+are\s+)?a",
        r"pretend\s+(you\s+are|to\s+be)",
        r"jailbreak",
        r"DAN\s+mode",
    ]
]

# PII patterns for output masking
_EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")
_PHONE_RE = re.compile(r"\b(\+?\d[\d\s\-().]{7,}\d)\b")
_SSN_RE   = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class GuardrailResult:
    passed: bool
    reason: str = ""


# ---------------------------------------------------------------------------
# Input guardrails
# ---------------------------------------------------------------------------

def check_input(query: str) -> GuardrailResult:
    """Validate a natural language query before sending to LLM."""
    from backend.config import get_settings
    max_len = get_settings().max_query_length

    if not query or not query.strip():
        return GuardrailResult(False, "Query cannot be empty")

    if len(query) > max_len:
        return GuardrailResult(False, f"Query exceeds maximum length of {max_len} characters")

    for pattern in _INJECTION_PATTERNS:
        if pattern.search(query):
            return GuardrailResult(False, "Query contains disallowed instructions")

    return GuardrailResult(True)


# ---------------------------------------------------------------------------
# SQL guardrails
# ---------------------------------------------------------------------------

def validate_sql(sql: str) -> GuardrailResult:
    """Ensure LLM-generated SQL is a safe SELECT statement."""
    if not sql or not sql.strip():
        return GuardrailResult(False, "Empty SQL returned by LLM")

    clean = sql.strip().lstrip(";").strip()

    if not clean.upper().startswith("SELECT"):
        return GuardrailResult(False, f"Only SELECT statements are allowed, got: {clean[:30]!r}")

    if _FORBIDDEN_SQL.search(clean):
        match = _FORBIDDEN_SQL.search(clean)
        return GuardrailResult(False, f"Forbidden keyword in SQL: {match.group()!r}")

    # Block stacked statements  (anything after first semicolon that isn't whitespace)
    stripped_stmts = [s.strip() for s in clean.split(";") if s.strip()]
    if len(stripped_stmts) > 1:
        return GuardrailResult(False, "Multiple SQL statements are not allowed")

    return GuardrailResult(True)


def extract_sql(raw: str) -> str:
    """
    Extract the SQL statement from LLM output that may contain
    markdown fences or surrounding explanation text.
    """
    # Remove ```sql ... ``` or ``` ... ``` fences
    fence = re.search(r"```(?:sql)?\s*([\s\S]+?)\s*```", raw, re.IGNORECASE)
    if fence:
        return fence.group(1).strip()

    # Try to find the first SELECT … line(s)
    lines = raw.strip().splitlines()
    sql_lines = []
    in_sql = False
    for line in lines:
        if line.strip().upper().startswith("SELECT") or in_sql:
            in_sql = True
            sql_lines.append(line)
    if sql_lines:
        return "\n".join(sql_lines).strip()

    return raw.strip()


# ---------------------------------------------------------------------------
# Output guardrails
# ---------------------------------------------------------------------------

_PII_COLUMNS = re.compile(r"\b(email|phone|mobile|ssn|tax_id|credit_card|password)\b", re.IGNORECASE)


def mask_pii_in_results(rows: list[dict]) -> list[dict]:
    """
    Mask PII values in result rows.
    - Columns whose names suggest PII are fully redacted.
    - Email/phone patterns in string values are masked.
    """
    if not rows:
        return rows

    pii_cols = {col for col in rows[0] if _PII_COLUMNS.search(col)}

    masked = []
    for row in rows:
        new_row = {}
        for k, v in row.items():
            if k in pii_cols:
                new_row[k] = "***"
            elif isinstance(v, str):
                v = _EMAIL_RE.sub("[email]", v)
                v = _PHONE_RE.sub("[phone]", v)
                v = _SSN_RE.sub("[ssn]", v)
                new_row[k] = v
            else:
                new_row[k] = v
        masked.append(new_row)
    return masked


def check_result_size(row_count: int) -> GuardrailResult:
    from backend.config import get_settings
    max_rows = get_settings().max_result_rows
    if row_count > max_rows:
        return GuardrailResult(False, f"Result exceeds max rows ({row_count} > {max_rows})")
    return GuardrailResult(True)
