# Text2Insight

> **Natural language → SQL → Insight.** Ask questions about your sales data in plain English. Get back tables, charts, and AI-written summaries — streamed live token by token.

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat&logo=fastapi&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=flat&logo=duckdb&logoColor=black)
![Groq](https://img.shields.io/badge/Groq-Llama_3.3_70B-F55036?style=flat&logoColor=white)
![JWT](https://img.shields.io/badge/JWT-Auth-000000?style=flat&logo=jsonwebtokens&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=flat)

---

## What It Does

Type: *"Which region had the highest revenue growth in Q3?"*

Text2Insight:
1. Converts your question to SQL using an LLM
2. Validates and executes the SQL against DuckDB
3. Renders a data table + auto-generated chart
4. Streams an AI-written business narrative — word by word, in real time

No SQL knowledge required. Results export to PDF, Word, or PowerPoint.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                    USER (Browser)                                │
│                                                                  │
│   "Which region had highest revenue growth in Q3?"               │
└──────────────────────────────┬───────────────────────────────────┘
                               │  HTTP POST /chat/query
                               │  Authorization: Bearer <JWT>
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                    FASTAPI BACKEND                               │
│                                                                  │
│  ┌─────────────┐   ┌──────────────┐   ┌─────────────────────┐   │
│  │  JWT Auth   │──►│ Rate Limiter │──►│  Input Guardrails   │   │
│  │  + Quota    │   │  20 req/min  │   │  Block SQL injection │   │
│  └─────────────┘   └──────────────┘   └──────────┬──────────┘   │
│                                                  │               │
│  ┌───────────────────────────────────────────────▼────────────┐  │
│  │                    CORE PIPELINE                           │  │
│  │                                                            │  │
│  │  1. Cache Lookup          ← skip LLM if seen before        │  │
│  │         │ miss                                             │  │
│  │         ▼                                                  │  │
│  │  2. Schema Injection      ← fetch DuckDB table metadata    │  │
│  │         │                                                  │  │
│  │         ▼                                                  │  │
│  │  3. NL→SQL (Groq primary) ← llama-3.3-70b-versatile        │  │
│  │         │  fail/timeout                                    │  │
│  │         ▼ (circuit breaker)                                │  │
│  │     NL→SQL (OpenAI fallback) ← gpt-4o-mini                │  │
│  │         │                                                  │  │
│  │         ▼                                                  │  │
│  │  4. SQL Validation        ← SELECT only, no writes allowed │  │
│  │         │                                                  │  │
│  │         ▼                                                  │  │
│  │  5. DuckDB Execution      ← runs validated SQL             │  │
│  │         │                                                  │  │
│  │         ▼                                                  │  │
│  │  6. Summarizer LLM        ← natural language insight       │  │
│  │         │                                                  │  │
│  │         ▼                                                  │  │
│  │  7. SSE Stream to browser ← token by token                 │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │                 MULTI-TURN MEMORY                          │  │
│  │  Stores chat history in SQLite per session                 │  │
│  │  Context window: last 10 turns injected into each prompt   │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
         Data Table         Chart         AI Summary
                                        (streamed via SSE)
              │                │                │
              └────────────────┼────────────────┘
                               ▼
                     Export: PDF / Word / PPT
```

---

## LLM Strategy — Groq + OpenAI Circuit Breaker

```
Every NL→SQL call:

  Primary: Groq llama-3.3-70b-versatile
    └── Fastest inference (~400ms avg)
    └── Lowest cost
    └── 8s timeout enforced

  Circuit Breaker:
    └── After 3 consecutive Groq failures → open circuit 60s
    └── All calls route to OpenAI fallback
    └── Circuit auto-resets after 60 seconds

  Fallback: OpenAI gpt-4o-mini
    └── Activates on: Groq 5xx, timeout >8s, rate limit 429
    └── More expensive but reliable

Result: users never see a failure — seamless provider switch
```

---

## SQL Safety Guardrails

```
Generated SQL before execution:

  Check 1: Must start with SELECT
  Check 2: No forbidden keywords
            INSERT | UPDATE | DELETE | DROP
            ALTER  | TRUNCATE | CREATE | EXEC
  Check 3: Max 500 rows returned
  Check 4: PII columns masked in output
            (email, phone, national_id patterns)

If any check fails → return error to user, no DB execution
```

---

## Streaming — Server-Sent Events (SSE)

```
Client opens EventSource connection to /chat/query

Server streams:
  data: {"type": "sql", "content": "SELECT region, SUM(revenue)..."}
  data: {"type": "table", "content": [[row1], [row2], ...]}
  data: {"type": "chart", "content": {"x": [...], "y": [...]}}
  data: {"type": "summary_token", "content": "The"}
  data: {"type": "summary_token", "content": " North"}
  data: {"type": "summary_token", "content": " region..."}
  data: {"type": "done"}

Result: user sees tokens appear word by word — feels instant,
        even for complex 3-second LLM responses.
```

---

## Tech Stack

| Layer | Technology | Detail |
|-------|-----------|--------|
| Backend | FastAPI + Python 3.11 | Async, all endpoints |
| Analytics DB | DuckDB | In-process, columnar, fast for analytics |
| App DB | SQLite via SQLAlchemy | Users, sessions, chat history, token ledger |
| LLM Primary | Groq — Llama 3.3 70B | NL→SQL + summarization |
| LLM Fallback | OpenAI GPT-4o-mini | Circuit-breaker fallback |
| Auth | JWT (access + refresh tokens) | 15-min access, 7-day refresh |
| Streaming | Server-Sent Events (SSE) | Real-time token streaming |
| Frontend | Vanilla JS, HTML5, CSS3 | Zero framework, fast load |
| Export | ReportLab, python-docx, python-pptx | PDF / Word / PPT |
| Testing | pytest | Unit + integration suite |

---

## Features

| Feature | Detail |
|---------|--------|
| NL→SQL | Plain English → DuckDB-compatible SQL via LLM |
| Streaming | Results stream token by token via SSE |
| Multi-turn memory | Context maintained across questions in a session |
| JWT Auth | Secure sessions with per-user token quota tracking |
| SQL confidence scoring | Low-confidence queries flagged before execution |
| Circuit breaker | Auto-fallback from Groq → OpenAI on failure |
| Export | PDF, Word, PowerPoint one-click download |
| Rate limiting | 20 queries/minute per user, sliding window |
| Test suite | Full pytest coverage: auth, pipeline, exports, guardrails |

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/auth/register` | Create account |
| POST | `/auth/login` | Get access + refresh tokens |
| POST | `/auth/refresh` | Renew access token |
| GET | `/auth/me` | Token quota remaining |
| POST | `/chat/query` | NL query → SSE stream |
| GET | `/chat/sessions` | List sessions |
| GET | `/chat/sessions/{id}/messages` | Full history |
| GET | `/export/{session_id}?format=pdf` | Download PDF |
| GET | `/health` | Liveness check |

---

## Quick Start

```bash
git clone https://github.com/iampriyabrat14/Text2Insight.git
cd Text2Insight
pip install -r requirements.txt
cp .env.example .env          # add your LLM API key
python -m backend.database.seed_data   # generate dummy sales data
uvicorn backend.main:app --reload
```

Open `http://localhost:8000` — log in and start querying.

### Environment Variables

```env
GROQ_API_KEY=your_groq_key
OPENAI_API_KEY=your_openai_key      # optional fallback
JWT_SECRET_KEY=your_secret_min_32_chars
SQLITE_URL=sqlite+aiosqlite:///./app.db
DUCKDB_PATH=./data/sales.duckdb
```

---

## Sales Data Schema (DuckDB)

Pre-seeded with realistic dummy data via `seed_data.py`:

| Table | Rows | Key Columns |
|-------|------|-------------|
| `customers` | 500 | region, segment, created_at |
| `products` | 200 | category, unit_price, cost |
| `orders` | 2,000 | order_date, status, channel |
| `order_items` | 5,000 | quantity, discount, sale_price |
| `sales_reps` | 50 | region, team, hire_date |
| `targets` | 200 | quarter, revenue_target, units_target |

---

## Project Structure

```
Text2Insight/
├── backend/
│   ├── main.py               # FastAPI entrypoint
│   ├── auth/                 # JWT, user model, token quota
│   ├── chat/                 # NL→SQL pipeline, session management
│   ├── llm/                  # Groq + OpenAI clients, circuit breaker
│   ├── database/             # DuckDB + SQLite managers, seed data
│   ├── export/               # PDF, Word, PPT generators
│   ├── cache/                # TTLCache (Redis-ready interface)
│   └── middleware/           # Rate limiter, latency logger
├── frontend/
│   ├── index.html            # Login / Register
│   ├── chat.html             # Main chat interface
│   └── js/                   # api.js, chat.js, export.js, quota.js
├── tests/
│   ├── test_auth.py
│   ├── test_nl_to_sql.py
│   ├── test_pipeline.py
│   └── test_export.py
├── requirements.txt
└── .env.example
```

---

## Interview Talking Points

**Q: How do you prevent SQL injection from LLM output?**
> The guardrail runs on the *generated SQL*, not the user's natural language input. It enforces: must start with SELECT, whitelist of safe keywords only, row cap of 500. The user's query is never executed — only the LLM's validated output is.

**Q: What if Groq goes down mid-production?**
> Circuit breaker pattern: after 3 consecutive failures, Groq is considered "open" for 60 seconds and all calls route to OpenAI gpt-4o-mini. Users see no error — the fallback is transparent.

**Q: How does SSE streaming work?**
> FastAPI returns a `StreamingResponse` with `text/event-stream` content type. The LLM response is streamed token by token using Groq's streaming API, and each token is immediately forwarded as an SSE `data:` event. The browser's `EventSource` API renders tokens as they arrive.

---

## License

MIT © 2026 Priyabrat Dalbehera

---

## Connect

**Priyabrat Dalbehera** — AI Engineer | Building production GenAI systems

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=flat&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/priyabrat-dalbehera-p521)
[![Portfolio](https://img.shields.io/badge/Portfolio-000000?style=flat&logo=vercel&logoColor=white)](https://www.aiwithpriyabrat.com/)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=flat&logo=github&logoColor=white)](https://github.com/iampriyabrat14)
[![Email](https://img.shields.io/badge/Email-D14836?style=flat&logo=gmail&logoColor=white)](mailto:ipriyabrat689@gmail.com)
