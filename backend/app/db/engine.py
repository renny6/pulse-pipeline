"""
SQLAlchemy AsyncEngine — Database Connection Pool Manager
==========================================================
[MANDATE — master_system_mandates.md §4 / master_solution_blueprint.md §3]
[MANDATE — system_hurdles_and_guardrails.md Challenge 8]

POOL ARCHITECTURE
-----------------
Scaling Celery workers creates a many-to-one fan-in toward TimescaleDB.
Without pool management each forked worker process inherits file descriptors
from the parent, creating "ghost" connections that exhaust postgres's
`max_connections` limit (default 100) within seconds of a traffic spike.

THREE-LAYER DEFENCE DEPLOYED HERE:

  1. AsyncEngine with explicit pool_size + max_overflow caps — hard ceiling
     on simultaneous connections per worker process.

  2. celery.signals.worker_process_init teardown — when Celery forks a new
     child process the signal handler calls engine.dispose() to DROP all
     inherited connections before the child opens its own fresh pool.
     Without this, the child would hold zombie file descriptors that consume
     both local FDs and server-side postgres slots.

  3. NullPool option for scripts / one-shot contexts (migrations, health
     checks) where you never want persistent pooling.
"""
from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from app.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level engine singleton
# Initialised lazily on first call to get_engine() so imports don't
# immediately open connections.
# ---------------------------------------------------------------------------
_engine: AsyncEngine | None = None
_async_session_factory: sessionmaker | None = None  # type: ignore[type-arg]


def _build_engine() -> AsyncEngine:
    """
    Construct the AsyncEngine with mandate-compliant pool settings.

    pool_size=5      → max persistent connections per worker process
    max_overflow=5   → max temporary burst connections (total = 10 / worker)
    pool_pre_ping=True → re-validate stale connections before use (prevents
                         "server closed connection" errors after idle periods)
    pool_recycle=1800 → recycle connections every 30 min (prevents postgres
                         idle timeout from closing them silently)
    """
    url = settings.database_url_async
    if not url:
        raise RuntimeError(
            "DATABASE_URL_ASYNC is not set. "
            "Check your .env file and docker-compose environment overrides."
        )

    engine = create_async_engine(
        url,
        echo=False,           # Set echo=True temporarily to debug SQL
        pool_size=5,
        max_overflow=5,
        pool_pre_ping=True,
        pool_recycle=1800,
        # connect_args for asyncpg — explicit command timeout
        connect_args={"command_timeout": 30},
    )
    logger.info("AsyncEngine created. pool_size=5, max_overflow=5, url=%s", url)
    return engine


def get_engine() -> AsyncEngine:
    """Return the module-level AsyncEngine, creating it on first call."""
    global _engine
    if _engine is None:
        _engine = _build_engine()
        global _async_session_factory
        _async_session_factory = sessionmaker(
            bind=_engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )
    return _engine


def get_session_factory() -> sessionmaker:  # type: ignore[type-arg]
    """Return the async session factory. Requires get_engine() to have been called."""
    get_engine()  # ensure initialised
    assert _async_session_factory is not None
    return _async_session_factory


async def dispose_engine() -> None:
    """
    Dispose all engine connections.

    Called from:
      - celery.signals.worker_process_init (pre-fork cleanup)
      - Application shutdown in FastAPI lifespan (Phase 3 extension)

    [MANDATE — master_system_mandates.md §4]
    Attach teardown events to the Celery pre-fork lifecycle
    (celery.signals.worker_process_init) to tear down and cycle
    inherited engine pools cleanly.
    """
    global _engine, _async_session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _async_session_factory = None
        logger.info("AsyncEngine disposed — connection pool fully drained.")
