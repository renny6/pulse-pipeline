"""
Pulse Gateway — FastAPI Application Entry Point
================================================
Assembles the full FastAPI application with:
  - Lifespan context for startup/shutdown resource management
  - Strict CORS middleware (no wildcard)
  - X-Correlation-ID middleware
  - API routers (ingestion, health)
  - WebSocket router

All architecture mandates are cross-referenced inline.
"""
from __future__ import annotations

import logging
import logging.config
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.core.rate_limiter import TokenBucketRateLimiter
from app.core.redis_client import close_redis, init_redis
from app.middleware.correlation_id import CorrelationIDMiddleware
from app.ws.manager import ws_manager


# ==============================================================================
# LOGGING SETUP
# [MANDATE — operations_and_observability_mandates.md §1]
# Log format includes correlation_id (injected via ContextVar by the middleware)
# so every log line from this gateway can be traced back to a request.
# ==============================================================================

class _CorrelationIDFilter(logging.Filter):
    """Injects the correlation_id ContextVar value into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        from app.middleware.correlation_id import correlation_id_ctx
        record.correlation_id = correlation_id_ctx.get()  # type: ignore[attr-defined]
        return True


def _configure_logging() -> None:
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "filters": {
                "correlation_id": {"()": _CorrelationIDFilter},
            },
            "formatters": {
                "pulse": {
                    "format": (
                        "%(asctime)s [%(correlation_id)s] "
                        "%(levelname)-8s %(name)s: %(message)s"
                    ),
                    "datefmt": "%Y-%m-%dT%H:%M:%S",
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "pulse",
                    "filters": ["correlation_id"],
                }
            },
            "root": {
                "level": settings.log_level.upper(),
                "handlers": ["console"],
            },
            # Quieten noisy third-party loggers
            "loggers": {
                "uvicorn.access": {"level": "WARNING"},
                "aiokafka": {"level": "WARNING"},
                "redis": {"level": "WARNING"},
            },
        }
    )


_configure_logging()
logger = logging.getLogger(__name__)


# ==============================================================================
# LIFESPAN — startup and graceful shutdown
# Modern FastAPI pattern (replaces deprecated @app.on_event).
# ==============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Manages the lifecycle of all shared resources:
      Startup:  Redis pool → Rate limiter → WebSocket broadcaster
      Shutdown: WS broadcaster → Redis pool
    """
    # ---- STARTUP ----
    logger.info("==> [Pulse Gateway] Starting up...")

    redis = await init_redis()

    # Initialise rate limiter and store on app.state for dependency injection
    app.state.rate_limiter = TokenBucketRateLimiter(
        redis_client=redis,
        capacity=settings.rate_limit_capacity,
        refill_rate=settings.rate_limit_refill_rate,
    )

    # Start the 100ms WebSocket broadcaster background task
    await ws_manager.startup()

    logger.info(
        "==> [Pulse Gateway] Ready. "
        "rate_limit_capacity=%d rate_limit_refill=%.1f t/s",
        settings.rate_limit_capacity,
        settings.rate_limit_refill_rate,
    )

    yield  # Application runs here

    # ---- SHUTDOWN ----
    logger.info("==> [Pulse Gateway] Shutting down...")
    await ws_manager.shutdown()
    await close_redis()
    logger.info("==> [Pulse Gateway] Shutdown complete.")


# ==============================================================================
# APPLICATION FACTORY
# ==============================================================================

def create_app() -> FastAPI:
    app = FastAPI(
        title="Pulse Distributed Ingestion Engine",
        description=(
            "High-throughput event ingestion gateway with atomic Redis Token Bucket "
            "rate limiting, WebSocket real-time metrics, and Kafka event streaming."
        ),
        version="1.0.0",
        lifespan=lifespan,
        # Disable automatic /docs and /redoc in a production hardening step.
        # For Phase 2 development, leave them enabled.
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # --------------------------------------------------------------------------
    # MIDDLEWARE STACK
    # Order matters: middlewares execute in REVERSE registration order.
    # CorrelationID must wrap CORS so the ID is available for error responses.
    # --------------------------------------------------------------------------

    # [MANDATE — zero_trust_security.md §4 / master_system_mandates.md §6]
    # Production CORS lockdown: NO wildcard. Only whitelisted dashboard origins.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Content-Type", "X-Correlation-ID", "Authorization"],
        expose_headers=["X-Correlation-ID"],
    )

    # [MANDATE — operations_and_observability_mandates.md §1]
    # X-Correlation-ID generation and propagation.
    app.add_middleware(CorrelationIDMiddleware)

    # --------------------------------------------------------------------------
    # ROUTERS
    # --------------------------------------------------------------------------
    from app.api.v1.health import router as health_router
    from app.api.v1.ingest import router as ingest_router
    from app.ws.endpoint import router as ws_router

    app.include_router(health_router)                        # /health, /health/ready, /api/v1/dev-ws-token
    app.include_router(ingest_router, prefix="/api/v1")      # /api/v1/ingest
    app.include_router(ws_router)                            # /ws/metrics

    return app


# Module-level app instance — imported by uvicorn CMD
app: FastAPI = create_app()
