"""
ASGI middleware that logs request method, path, status code, and latency.
Adds an X-Process-Time header to every response.
"""
import time
import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger("latency")


class LatencyLoggerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        t0 = time.perf_counter()
        response = await call_next(request)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        response.headers["X-Process-Time"] = f"{elapsed_ms:.1f}ms"
        logger.info(
            "%s %s → %d  (%.1fms)",
            request.method, request.url.path, response.status_code, elapsed_ms,
        )
        return response
