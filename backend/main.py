"""
FastAPI application entry point.
Lifespan: initialises SQLite tables, seeds DuckDB, opens DuckDB connection.
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from backend.middleware.latency_logger import LatencyLoggerMiddleware

from backend.config import get_settings
from backend.database.sqlite_manager import init_db
from backend.database.duckdb_manager import init_duckdb, close_duckdb
from backend.database.seed_data import async_seed_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logger.info("Starting up...")

    # 1. Create SQLite tables
    await init_db()
    logger.info("SQLite tables ready")

    # 2. Seed default users (no-op if already exist)
    from backend.database.sqlite_manager import get_session_factory
    from backend.auth.service import create_user
    factory = get_session_factory()
    async with factory() as db:
        for username, email, password, tier in [
            ("admin", "admin@example.com", "admin123", "admin"),
            ("demo",  "demo@example.com",  "demo123",  "pro"),
        ]:
            try:
                await create_user(db, username, email, password, tier)
                logger.info("Created default user: %s", username)
            except ValueError:
                pass  # already exists

    # 3. Seed DuckDB (no-op if already seeded)
    await async_seed_all(settings.duckdb_path)

    # 3. Open persistent DuckDB connection
    init_duckdb()

    yield

    # Shutdown
    close_duckdb()
    logger.info("Shutdown complete")


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title="SQL-2-NLP Chatbot",
        version="1.0.0",
        description="Natural language to SQL chatbot with DuckDB, Groq, and export support",
        lifespan=lifespan,
        docs_url="/api/docs",
        redoc_url="/api/redoc",
    )

    app.add_middleware(LatencyLoggerMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins_list,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    from backend.auth.router import router as auth_router
    from backend.chat.router import router as chat_router
    from backend.export.router import router as export_router
    from backend.upload.router import router as upload_router
    app.include_router(auth_router, prefix="/auth", tags=["auth"])
    app.include_router(chat_router, prefix="/chat", tags=["chat"])
    app.include_router(export_router, prefix="/export", tags=["export"])
    app.include_router(upload_router, prefix="/upload", tags=["upload"])

    # Routers registered as each module is implemented:
    # from backend.evaluation.eval_router import router as eval_router
    # app.include_router(eval_router, prefix="/eval", tags=["eval"])

    @app.get("/health", tags=["health"])
    async def health():
        return {"status": "ok"}

    @app.get("/health/ready", tags=["health"])
    async def readiness():
        from backend.database.duckdb_manager import get_schema
        try:
            schema = await get_schema()
            tables = list(schema.keys())
        except Exception as exc:
            return {"status": "error", "detail": str(exc)}, 503
        return {"status": "ready", "duckdb_tables": tables}

    # Serve frontend — must be mounted last so API routes take precedence
    from pathlib import Path
    frontend_dir = Path(__file__).parent.parent / "frontend"
    if frontend_dir.exists():
        from fastapi.responses import FileResponse

        @app.get("/", include_in_schema=False)
        async def root():
            return FileResponse(str(frontend_dir / "index.html"))

        @app.get("/chat", include_in_schema=False)
        async def chat_page():
            return FileResponse(str(frontend_dir / "chat.html"))

        app.mount("/", StaticFiles(directory=str(frontend_dir)), name="static")

    return app


app = create_app()
