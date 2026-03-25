# CLAUDE.md — SQL-to-NLP Chatbot Platform

## Project Overview

A production-grade, multi-user chatbot that converts natural language queries into SQL, fetches data from DuckDB, summarizes results, and exports them to PDF/Word/PPT. Built with FastAPI + Vanilla JS, using Groq (primary) and OpenAI (fallback) as LLM providers.

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Frontend | HTML5 + CSS3 + Vanilla JS | SPA-style chat UI, auth forms, export controls |
| Backend | FastAPI (Python 3.11+) | REST API, async request handling |
| Analytics DB | DuckDB (in-process) | Query execution on sales data |
| App DB | SQLite (via SQLAlchemy) | Users, sessions, chat history, token ledger |
| LLM Primary | Groq (llama-3.3-70b-versatile) | NL→SQL + summarization (low latency) |
| LLM Fallback | OpenAI (gpt-4o-mini) | Circuit-breaker fallback when Groq fails |
| Cache | Redis (or in-memory TTLCache) | Query result + LLM response caching |
| Export | ReportLab (PDF), python-docx (Word), python-pptx (PPT) | Document generation |
| Guardrails | Custom middleware + Presidio (optional) | Input/output safety checks |
| Auth | JWT (access + refresh tokens) | Stateless auth, per-user token tracking |
| Eval | Ragas / custom metrics | Response quality evaluation |
| Task Queue | asyncio background tasks (or Celery if scale needed) | Async export generation |

---

## Project Structure

```
sql-2-nlp/
├── backend/
│   ├── main.py                    # FastAPI app entry point, lifespan hooks
│   ├── config.py                  # Settings via pydantic-settings (.env)
│   ├── dependencies.py            # Shared FastAPI dependencies (DB, auth)
│   │
│   ├── auth/
│   │   ├── router.py              # /auth/register, /auth/login, /auth/refresh
│   │   ├── models.py              # SQLAlchemy User model
│   │   ├── schemas.py             # Pydantic request/response schemas
│   │   ├── service.py             # Password hashing, JWT creation/validation
│   │   └── token_ledger.py        # Per-user token quota management
│   │
│   ├── chat/
│   │   ├── router.py              # /chat/query, /chat/history, /chat/sessions
│   │   ├── models.py              # ChatSession, ChatMessage SQLAlchemy models
│   │   ├── schemas.py             # Chat request/response schemas
│   │   └── service.py             # Orchestrates NL→SQL→fetch→summarize pipeline
│   │
│   ├── llm/
│   │   ├── client.py              # Groq + OpenAI clients, circuit-breaker logic
│   │   ├── nl_to_sql.py           # NL→SQL prompt engineering + schema injection
│   │   ├── summarizer.py          # Result summarization prompts
│   │   └── guardrails.py          # Input sanitization, output validation, PII check
│   │
│   ├── database/
│   │   ├── duckdb_manager.py      # DuckDB connection, query execution, schema fetcher
│   │   ├── sqlite_manager.py      # SQLAlchemy engine + session factory
│   │   └── seed_data.py           # Generate dummy sales DataFrames → DuckDB
│   │
│   ├── export/
│   │   ├── router.py              # /export/{session_id}?format=pdf|word|ppt
│   │   ├── pdf_exporter.py        # ReportLab PDF generation
│   │   ├── word_exporter.py       # python-docx Word generation
│   │   └── ppt_exporter.py        # python-pptx PPT generation
│   │
│   ├── cache/
│   │   └── cache_manager.py       # TTLCache wrapper (Redis-ready interface)
│   │
│   ├── evaluation/
│   │   ├── metrics.py             # Latency, token usage, SQL validity, answer quality
│   │   └── eval_router.py         # /eval/report endpoint (admin only)
│   │
│   └── middleware/
│       ├── rate_limiter.py        # Sliding-window rate limiter per user
│       ├── latency_logger.py      # Request timing middleware
│       └── error_handler.py       # Global exception handlers
│
├── frontend/
│   ├── index.html                 # Login/Register page
│   ├── chat.html                  # Main chat interface
│   ├── css/
│   │   ├── main.css               # Global styles, CSS variables, dark/light theme
│   │   ├── auth.css               # Auth form styles
│   │   └── chat.css               # Chat bubbles, sidebar, export panel
│   └── js/
│       ├── api.js                 # Fetch wrapper, JWT header injection, token refresh
│       ├── auth.js                # Login/register/logout logic
│       ├── chat.js                # Send message, render response, chat history
│       ├── export.js              # Export button handlers, download trigger
│       └── quota.js               # Display token usage bar / remaining tokens
│
├── data/
│   └── sales.duckdb               # Auto-created on first run by seed_data.py
│
├── exports/                       # Temp storage for generated files
│
├── tests/
│   ├── test_auth.py
│   ├── test_nl_to_sql.py
│   ├── test_pipeline.py
│   ├── test_export.py
│   └── test_guardrails.py
│
├── .env.example
├── requirements.txt
├── docker-compose.yml             # Optional: app + Redis
└── CLAUDE.md
```

---

## Dummy Sales Data Schema (DuckDB)

Generate with `pandas` and `Faker`, load into DuckDB at startup via `seed_data.py`.

### Tables

| Table | Key Columns | Rows |
|---|---|---|
| `customers` | customer_id, name, email, region, segment, created_at | 500 |
| `products` | product_id, name, category, sub_category, unit_price, cost | 200 |
| `orders` | order_id, customer_id, order_date, status, channel, region | 2000 |
| `order_items` | item_id, order_id, product_id, quantity, discount, sale_price | 5000 |
| `sales_reps` | rep_id, name, region, team, hire_date | 50 |
| `targets` | target_id, rep_id, year, quarter, revenue_target, units_target | 200 |

### Relationships
- `orders.customer_id` → `customers.customer_id`
- `order_items.order_id` → `orders.order_id`
- `order_items.product_id` → `products.product_id`
- `orders.rep_id` → `sales_reps.rep_id`
- `targets.rep_id` → `sales_reps.rep_id`

---

## Authentication & Token Quota System

### Auth Flow
1. Register → bcrypt-hashed password stored in SQLite `users` table
2. Login → returns `access_token` (15 min TTL) + `refresh_token` (7 day TTL)
3. Every protected endpoint validates JWT via `Authorization: Bearer <token>`
4. Refresh endpoint issues new access token using refresh token

### Token Quota
- Each user gets a configurable `token_limit` (default: 100,000 tokens/month)
- SQLite table `token_ledger`: `(user_id, month, tokens_used)`
- Before each LLM call: check quota → deduct on success → return `429` if exceeded
- Quota resets monthly (cron job or check-on-request logic)
- Admin role: unlimited tokens, can view all user usage

### User Tiers (configurable in `.env`)
| Tier | Monthly Token Limit |
|---|---|
| free | 10,000 |
| basic | 50,000 |
| pro | 200,000 |
| admin | unlimited |

---

## Core Pipeline: NL → SQL → Fetch → Summarize

```
User Input (natural language)
        │
        ▼
[Guardrails: Input Check]      ← block SQL injection attempts, prompt injection, PII
        │
        ▼
[Cache Lookup]                 ← return cached result if query seen before (TTL: 5 min)
        │ miss
        ▼
[Schema Injection]             ← fetch DuckDB table/column metadata dynamically
        │
        ▼
[NL → SQL (Groq)]              ← few-shot prompt with schema + user query
        │  failure/timeout
        ▼ (circuit breaker)
[NL → SQL (OpenAI fallback)]
        │
        ▼
[SQL Validation]               ← syntax check, block writes (INSERT/UPDATE/DELETE/DROP)
        │
        ▼
[DuckDB Query Execution]       ← execute validated SELECT, fetch DataFrame
        │
        ▼
[Result Guardrail]             ← cap rows returned (max 500), mask PII columns
        │
        ▼
[Summarizer LLM]               ← Groq/OpenAI summarizes the result in natural language
        │
        ▼
[Cache Store]                  ← store (query_hash → summary + raw_result)
        │
        ▼
[Save to SQLite]               ← persist message, SQL, result, tokens used
        │
        ▼
Response to User + Token Deduction
```

---

## LLM Client — Circuit Breaker & Fallback

### Strategy
- **Primary**: Groq `llama-3.3-70b-versatile` — fastest, cheapest
- **Fallback**: OpenAI `gpt-4o-mini` — activates when:
  - Groq returns HTTP 5xx
  - Groq latency > 8 seconds (timeout)
  - Groq rate limit hit (HTTP 429)
- Circuit breaker: after 3 consecutive Groq failures → open circuit for 60 seconds, route all to OpenAI

### Prompt Strategy for NL→SQL
```
System: You are a SQL expert. Given the schema below, write a single valid DuckDB SQL SELECT query.
        Schema: {schema_json}
        Rules:
        - Only SELECT statements allowed
        - Use table aliases
        - Limit results to 500 rows unless user specifies
        - Return ONLY the SQL, no explanation

User: {natural_language_query}
```

### Prompt Strategy for Summarization
```
System: You are a data analyst. Summarize the following query result clearly and concisely.
        Highlight key trends, totals, and anomalies.
        Result: {dataframe_as_markdown_table}

User: Summarize this for: "{original_user_query}"
```

---

## Guardrails

### Input Guardrails
- Reject queries containing raw SQL keywords with intent to modify data (`DROP`, `DELETE`, `INSERT`, `UPDATE`, `TRUNCATE`, `ALTER`)
- Detect prompt injection patterns (e.g., "ignore previous instructions")
- Length check: max 1000 characters per query
- Rate limit: max 20 queries/minute per user (sliding window in Redis/memory)

### Output Guardrails
- Validate generated SQL is a pure `SELECT` before execution
- Cap result rows at 500
- Detect and redact PII patterns in results (emails, phone numbers) via regex or Presidio
- If summary contains hallucinated numbers not in the data → log warning (spot-check via regex)

### SQL Safety Validation
```python
# Pseudocode — implement in llm/guardrails.py
def validate_sql(sql: str) -> bool:
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "CREATE", "EXEC"]
    upper_sql = sql.strip().upper()
    if not upper_sql.startswith("SELECT"):
        return False
    for keyword in forbidden:
        if keyword in upper_sql:
            return False
    return True
```

---

## Caching Strategy

### Cache Keys
- NL→SQL cache: `md5(schema_version + normalized_query)`
- Result cache: `md5(sql_query)`
- Schema cache: `"duckdb_schema"` (invalidate on data reload)

### TTLs
| Cache Type | TTL |
|---|---|
| Schema metadata | 30 minutes |
| SQL query result | 5 minutes |
| LLM-generated SQL | 10 minutes |
| Summarization | 5 minutes |

### Implementation
- Default: `cachetools.TTLCache` (in-process, no dependencies)
- Production upgrade: Redis via `aioredis` (drop-in via cache_manager interface)
- Cache hit → skip LLM call → no tokens deducted

---

## Export System

### Endpoints
- `GET /export/{session_id}?format=pdf` — PDF via ReportLab
- `GET /export/{session_id}?format=word` — DOCX via python-docx
- `GET /export/{session_id}?format=ppt` — PPTX via python-pptx

### Export Content Per Format
| Format | Contents |
|---|---|
| PDF | Title, user query, generated SQL, result table, summary, metadata (date, user, tokens) |
| Word | Same as PDF with styled headings, table with borders, summary paragraph |
| PPT | Slide 1: Title. Slide 2: Query + SQL. Slide 3: Result table. Slide 4: Summary. Slide 5: Key metrics |

### Export Flow
1. Fetch chat session + all messages from SQLite
2. Filter messages that have `result_data` attached
3. Generate file in-memory → stream as download response
4. Clean up temp file after 10 minutes (background task)

---

## Evaluation & Metrics

### Runtime Metrics (logged per query)
| Metric | How Measured |
|---|---|
| End-to-end latency | `time.perf_counter()` around full pipeline |
| LLM latency | Separate timing around LLM call |
| SQL execution time | DuckDB execute timing |
| Token usage | From LLM API response `usage` field |
| Cache hit rate | Increment counter on hit/miss |
| SQL validity rate | Count valid vs invalid SQL generated |
| Fallback rate | Count Groq vs OpenAI calls |

### Quality Metrics (async/offline)
| Metric | Method |
|---|---|
| SQL correctness | Run expected SQL vs generated SQL on test fixtures |
| Answer relevance | Cosine similarity of summary vs reference (Ragas) |
| Faithfulness | Check summary claims against raw result data |
| Context precision | Whether SQL selected correct columns |

### Eval Endpoint
- `GET /eval/report` (admin only) — returns JSON with aggregated metrics
- `GET /eval/latency-percentiles` — p50, p95, p99 latency breakdown

### Metrics Storage
- All metrics written to SQLite `query_metrics` table
- Fields: `query_id, user_id, latency_ms, llm_latency_ms, sql_latency_ms, tokens_used, cache_hit, llm_provider, sql_valid, timestamp`

---

## Scalability Design

### For Large User Loads
- **Async FastAPI**: all endpoints use `async def`, DuckDB queries run in `asyncio.run_in_executor` (thread pool) since DuckDB is not async-native
- **Connection pooling**: SQLAlchemy async engine with pool size tunable via `.env`
- **DuckDB**: single shared connection with thread-safe locking for reads; DuckDB supports concurrent reads natively
- **Stateless backend**: JWT-based auth means any instance can serve any user
- **Horizontal scaling**: deploy multiple FastAPI instances behind nginx/load balancer; shared Redis for cache and rate limiting
- **Export offloading**: heavy export generation runs as background tasks, user polls or uses WebSocket for completion notification

### Rate Limiting (per user)
- Sliding window: 20 requests/minute
- Implemented in `middleware/rate_limiter.py` using in-memory dict (Redis for multi-instance)
- Returns `HTTP 429` with `Retry-After` header

---

## Frontend Design

### Pages
1. **index.html** — Login + Register tabs, token quota display post-login
2. **chat.html** — Main interface

### Chat UI Components
- **Left sidebar**: session history list, new chat button, logout
- **Main area**: chat bubbles (user = right, bot = left), SQL disclosure (collapsible), result table preview
- **Bottom bar**: text input, send button, token counter (remaining/total)
- **Export panel**: appears after each bot response with PDF/Word/PPT download buttons
- **Token bar**: progress bar showing token usage (color: green → yellow → red)

### JS Architecture
- `api.js`: central fetch wrapper — injects `Authorization` header, handles `401` by auto-refreshing token, handles `429` by showing quota warning
- `chat.js`: manages message rendering, calls `/chat/query`, renders markdown in summary
- `export.js`: calls `/export/{session_id}?format=X`, triggers browser download
- `quota.js`: polls `/auth/me` for updated token usage, updates progress bar

### No Framework Rule
- Pure DOM manipulation via `document.createElement`, `classList`, `innerHTML`
- No jQuery, no React, no bundler
- CSS custom properties for theming, CSS Grid/Flexbox for layout

---

## API Endpoints Summary

### Auth
| Method | Path | Description |
|---|---|---|
| POST | `/auth/register` | Create account |
| POST | `/auth/login` | Get access + refresh tokens |
| POST | `/auth/refresh` | Refresh access token |
| GET | `/auth/me` | Current user info + token quota |

### Chat
| Method | Path | Description |
|---|---|---|
| POST | `/chat/query` | Main pipeline: NL→SQL→fetch→summarize |
| GET | `/chat/sessions` | List user's chat sessions |
| GET | `/chat/sessions/{id}/messages` | Full message history for session |
| DELETE | `/chat/sessions/{id}` | Delete session + messages |

### Export
| Method | Path | Description |
|---|---|---|
| GET | `/export/{session_id}` | Download export (`?format=pdf\|word\|ppt`) |

### Eval (admin)
| Method | Path | Description |
|---|---|---|
| GET | `/eval/report` | Aggregated quality + performance metrics |
| GET | `/eval/latency-percentiles` | Latency p50/p95/p99 |

### Health
| Method | Path | Description |
|---|---|---|
| GET | `/health` | Liveness check |
| GET | `/health/ready` | Readiness check (DB + LLM connectivity) |

---

## Environment Variables (`.env`)

```env
# LLM
GROQ_API_KEY=
OPENAI_API_KEY=
GROQ_MODEL=llama-3.3-70b-versatile
OPENAI_FALLBACK_MODEL=gpt-4o-mini
LLM_TIMEOUT_SECONDS=8
CIRCUIT_BREAKER_THRESHOLD=3
CIRCUIT_BREAKER_RESET_SECONDS=60

# Auth
JWT_SECRET_KEY=
JWT_ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=15
REFRESH_TOKEN_EXPIRE_DAYS=7

# Token Quotas
DEFAULT_MONTHLY_TOKEN_LIMIT=10000
FREE_TIER_LIMIT=10000
BASIC_TIER_LIMIT=50000
PRO_TIER_LIMIT=200000

# Database
SQLITE_URL=sqlite+aiosqlite:///./app.db
DUCKDB_PATH=./data/sales.duckdb

# Cache
CACHE_TTL_SCHEMA=1800
CACHE_TTL_RESULT=300
CACHE_TTL_SQL=600
REDIS_URL=                        # optional; falls back to in-memory if empty

# Rate Limiting
RATE_LIMIT_PER_MINUTE=20
MAX_RESULT_ROWS=500
MAX_QUERY_LENGTH=1000

# Export
EXPORT_TEMP_DIR=./exports
EXPORT_CLEANUP_MINUTES=10

# App
DEBUG=false
LOG_LEVEL=INFO
CORS_ORIGINS=http://localhost:3000,http://127.0.0.1:5500
```

---

## Dependencies (`requirements.txt`)

```
# Web framework
fastapi>=0.111.0
uvicorn[standard]>=0.30.0
python-multipart>=0.0.9

# Database
sqlalchemy[asyncio]>=2.0.30
aiosqlite>=0.20.0
duckdb>=0.10.3
pandas>=2.2.0
faker>=25.0.0

# Auth
python-jose[cryptography]>=3.3.0
passlib[bcrypt]>=1.7.4

# LLM
groq>=0.9.0
openai>=1.35.0

# Cache
cachetools>=5.3.3
aioredis>=2.0.1          # optional, for Redis

# Export
reportlab>=4.2.0
python-docx>=1.1.2
python-pptx>=0.6.23

# Evaluation
ragas>=0.1.14             # optional, for quality metrics

# Guardrails (optional)
presidio-analyzer>=2.2.354
presidio-anonymizer>=2.2.354

# Utilities
pydantic-settings>=2.3.0
python-dotenv>=1.0.1
httpx>=0.27.0

# Testing
pytest>=8.2.0
pytest-asyncio>=0.23.7
httpx>=0.27.0             # for TestClient
```

---

## Development Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Seed DuckDB with dummy data
python -m backend.database.seed_data

# Run backend (dev)
uvicorn backend.main:app --reload --port 8000

# Serve frontend (simple static server)
python -m http.server 3000 --directory frontend/

# Run tests
pytest tests/ -v

# Run with Docker Compose (includes Redis)
docker-compose up --build
```

---

## Implementation Order

1. **Data layer**: `seed_data.py` → generate DataFrames → persist to DuckDB
2. **SQLite models**: User, ChatSession, ChatMessage, TokenLedger, QueryMetrics
3. **Auth system**: register/login/refresh + token quota middleware
4. **LLM client**: Groq + OpenAI clients + circuit breaker + fallback logic
5. **NL→SQL pipeline**: schema fetcher → prompt builder → SQL validator → executor
6. **Summarizer**: result → LLM summary
7. **Guardrails**: input/output checks, rate limiter middleware
8. **Cache layer**: TTLCache wrapper, integrate into pipeline
9. **Export system**: PDF + Word + PPT generators + export router
10. **Evaluation**: metrics logger, eval endpoints
11. **Frontend**: auth pages → chat UI → export buttons → token quota display
12. **Integration testing**: full pipeline tests, load test with locust

---

## Security Considerations

- Passwords: bcrypt with cost factor 12
- JWT: short-lived access tokens (15 min), rotate refresh tokens on use
- SQL: whitelist-only SELECT; parameterized execution in DuckDB
- CORS: explicit origins list, no wildcard in production
- File exports: sanitize session IDs in file paths to prevent path traversal
- Rate limiting: per-user, not just per-IP, to prevent abuse after auth
- Secrets: all keys in `.env`, never committed; `.env.example` checked in

---

## Notes for Claude

- DuckDB is not async-native — always wrap DuckDB calls in `asyncio.run_in_executor` with a `ThreadPoolExecutor`
- SQLite with `aiosqlite` is fully async-compatible; use `async_sessionmaker`
- JWT refresh logic in `api.js` must handle token expiry transparently (intercept 401, refresh, retry)
- Export files must stream directly as `StreamingResponse` or `FileResponse`, not loaded into memory for large results
- Cache manager must present a uniform interface so Redis can replace in-memory cache with zero changes to calling code
- Guardrail for SQL injection must run AFTER LLM generation, not before — the LLM input is natural language, not SQL
- Groq API is OpenAI-compatible; the `groq` Python SDK mirrors the `openai` SDK interface
