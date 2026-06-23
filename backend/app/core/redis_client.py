"""
Redis Connection Pool Manager
==============================
Manages the shared async Redis client for the entire FastAPI application.

All components — rate limiter, cursor checkpointing, outbound API token
tracking — share a single connection pool to avoid connection exhaustion.

Pool lifecycle is managed via the FastAPI lifespan context manager in main.py.
"""
from __future__ import annotations

import logging

import redis.asyncio as aioredis

from app.config import settings

logger = logging.getLogger(__name__)

# Module-level singleton — initialised in app lifespan, used via get_redis()
_redis_client: aioredis.Redis | None = None


async def init_redis() -> aioredis.Redis:
    """
    Initialise the Redis connection pool.
    Called once at application startup inside the lifespan context manager.
    """
    global _redis_client

    _redis_client = aioredis.from_url(
        settings.redis_url,
        encoding="utf-8",
        decode_responses=True,
        # Pool size — generous enough for rate limiter + WS + future cursor use
        max_connections=30,
        socket_connect_timeout=5,
        socket_timeout=5,
        retry_on_timeout=True,
    )

    # Verify connectivity before declaring the app ready
    await _redis_client.ping()
    logger.info("✓ Redis connection pool ready: %s", settings.redis_url)
    return _redis_client


async def close_redis() -> None:
    """Drain and close the Redis connection pool at application shutdown."""
    global _redis_client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None
        logger.info("Redis connection pool closed.")


def get_redis() -> aioredis.Redis:
    """
    Synchronous accessor for the shared Redis client.
    Safe to call after init_redis() has been awaited in lifespan.

    Used as a FastAPI dependency:
        redis = Depends(get_redis)
    """
    if _redis_client is None:
        raise RuntimeError(
            "Redis client has not been initialised. "
            "Ensure init_redis() is called in the app lifespan."
        )
    return _redis_client
