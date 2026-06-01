# Text2Insight 🔍

> **Natural language → SQL → Insight.** Ask questions about your sales data. Get back tables, charts, and AI-written summaries — streamed in real time.

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat&logo=fastapi&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=flat&logo=duckdb&logoColor=black)
![JWT](https://img.shields.io/badge/JWT-Auth-000000?style=flat&logo=jsonwebtokens&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=flat)

---

## What It Does

Text2Insight converts plain English questions into SQL, executes them against a DuckDB sales database, and returns:

- **Data table** — structured query results
- **Chart** — auto-rendered visualization
- **AI summary** — business-friendly insight narrative
- All **streamed live** via Server-Sent Events (SSE)

---

## Architecture

```
User Query
    │
    ▼
┌──────────────┐     JWT Auth
│   FastAPI    │ ◄──────────────── User Login
│   Backend    │
└──────┬───────┘
       │
  ┌────┴────┐
  ▼         ▼
SQL        Memory
Generator  Manager
(LLM)    (Multi-turn)
  │
  ▼
DuckDB Execution
  │
  ├── Table Results
  ├── Chart Data
  └── AI Summary (streamed via SSE)
       │
       ▼
  Export: PDF / Word / PowerPoint
```

---

## Features

- **NL-to-SQL** — Converts natural language to DuckDB-compatible SQL
- **SSE Streaming** — Results stream token by token in real time
- **Multi-turn Memory** — Maintains conversation context across questions
- **JWT Authentication** — Secure user sessions with token quota management
- **SQL Confidence Scoring** — Flags low-confidence queries before execution
- **Export** — Download results as PDF, Word, or PowerPoint
- **Test Suite** — Full pytest suite with `tests/` directory

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | FastAPI, Python |
| Database | DuckDB |
| Auth | JWT (JSON Web Tokens) |
| Streaming | Server-Sent Events (SSE) |
| Frontend | Vanilla JavaScript, HTML5, CSS3 |
| Testing | pytest |
| Export | PDF, DOCX, PPTX generation |

---

## Quick Start

```bash
git clone https://github.com/iampriyabrat14/Text2Insight.git
cd Text2Insight
pip install -r requirements.txt
cp .env.example .env   # Add your LLM API key
uvicorn backend.main:app --reload
```

Open `http://localhost:8000` — log in and start querying your data.

---

## API Endpoints (Key)

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/auth/login` | JWT login |
| POST | `/query/stream` | NL query → SSE stream |
| GET | `/query/history` | Conversation history |
| POST | `/export/pdf` | Export results as PDF |

---

## Project Structure

```
Text2Insight/
├── backend/           # FastAPI app
├── frontend/          # Vanilla JS single-page UI
├── tests/             # pytest test suite
├── CLAUDE.md          # Dev notes
├── requirements.txt
└── .env.example
```

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