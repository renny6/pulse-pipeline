"""
Health Check Endpoints
=======================
Provides two health probes:

  /health         → Liveness probe. Always returns 200 if the process is alive.
                    Used by Docker healthcheck and load balancers.

  /health/ready   → Readiness probe. Returns 200 only if all critical
                    dependencies (Redis) are reachable. If Redis is down,
                    returns 503 so the load balancer stops routing traffic here.

  /api/v1/dev-ws-token → DEV ONLY. Returns a signed JWT for WebSocket auth.
                          In production this would be removed or protected.
"""
from __future__ import annotations

import logging

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.config import settings
from app.core.redis_client import get_redis
from app.core.security import create_dev_ws_token

logger = logging.getLogger(__name__)

router = APIRouter()


# ==============================================================================
# Response models
# ==============================================================================

class LivenessResponse(BaseModel):
    status: str
    service: str
    version: str


class ReadinessResponse(BaseModel):
    status: str
    redis: str


class DevTokenResponse(BaseModel):
    token: str
    warning: str


# ==============================================================================
# Liveness probe — always 200 while the process is alive
# ==============================================================================

@router.get(
    "/health",
    response_model=LivenessResponse,
    summary="Liveness probe",
    tags=["Health"],
)
async def liveness() -> LivenessResponse:
    """Returns 200 while the FastAPI process is running."""
    return LivenessResponse(
        status="alive",
        service="pulse-gateway",
        version="1.0.0",
    )


class ReadinessSystemResponse(BaseModel):
    status: str
    redis: str
    postgres: str
    kafka: str

# ==============================================================================
# Readiness probe — checks Redis connectivity
# ==============================================================================

@router.get(
    "/health/ready",
    response_model=ReadinessResponse,
    summary="Readiness probe",
    tags=["Health"],
)
async def readiness(redis: aioredis.Redis = Depends(get_redis)) -> ReadinessResponse:
    """
    Returns 200 only when the gateway can reach Redis.
    Returns 503 Service Unavailable if Redis is down.

    Used by Docker healthcheck and upstream load balancers to stop routing
    traffic to a degraded instance.
    """
    try:
        await redis.ping()
        redis_status = "ok"
    except Exception as exc:
        logger.error("Readiness check: Redis unavailable — %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"status": "degraded", "redis": "unreachable"},
        ) from exc

    return ReadinessResponse(status="ready", redis=redis_status)

# ==============================================================================
# System Health probe — checks ALL dependent services
# ==============================================================================

@router.get(
    "/health/system",
    response_model=ReadinessSystemResponse,
    summary="System Health probe",
    tags=["Health"],
)
async def system_health(redis: aioredis.Redis = Depends(get_redis)) -> ReadinessSystemResponse:
    """
    Detailed health check for the UI widget. Checks Redis, Postgres, and Kafka.
    """
    redis_status = "unreachable"
    postgres_status = "unreachable"
    kafka_status = "unreachable"
    
    # 1. Check Redis
    try:
        await redis.ping()
        redis_status = "ok"
    except Exception:
        pass

    # 2. Check Postgres
    try:
        from app.db.engine import get_engine
        # Synchronous check using asyncpg isn't ideal here, but we can do a quick async check
        # Actually, get_engine() is synchronous, but the session is async.
        from app.db.engine import get_session_factory
        session_factory = get_session_factory()
        from sqlalchemy import text
        async with session_factory() as session:
            await session.execute(text("SELECT 1"))
        postgres_status = "ok"
    except Exception:
        pass

    # 3. Check Kafka
    try:
        # Since we just want a quick ping, we can use the app's producer if available
        # But a robust way is checking the producer state or sending a dummy request.
        # For simplicity in this widget, we'll rely on producer being injected if available,
        # or we just try a quick socket connect.
        # A simpler way: just check if producer is connected.
        # The producer is in request.app.state.kafka_producer
        pass
    except Exception:
        pass

    # A better approach for Kafka: check if the global producer has partitions for pulse.events.raw
    # To keep it completely independent, let's just use aiokafka AIOKafkaProducer to connect and get metadata, or simpler, we can just use asyncio.open_connection to the broker port
    try:
        from app.config import settings
        host, port = settings.kafka_broker_url.split(":")
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, int(port)), timeout=2.0)
        writer.close()
        await writer.wait_closed()
        kafka_status = "ok"
    except Exception:
        pass

    overall = "ready" if redis_status == "ok" and postgres_status == "ok" and kafka_status == "ok" else "degraded"
    
    return ReadinessSystemResponse(
        status=overall,
        redis=redis_status,
        postgres=postgres_status,
        kafka=kafka_status
    )


# ==============================================================================
# Dev-only: WebSocket JWT token generator
# ==============================================================================

@router.get(
    "/api/v1/dev-ws-token",
    response_model=DevTokenResponse,
    summary="[DEV ONLY] Generate a WebSocket JWT",
    tags=["Development"],
)
async def dev_ws_token() -> DevTokenResponse:
    """
    Issues a 24-hour signed JWT for connecting to /ws/metrics.

    [WARNING] This endpoint is for LOCAL DEVELOPMENT ONLY.
    Remove or protect it behind auth middleware before production deployment.
    In production, short-lived tokens (≤15 minutes) should be issued by
    a proper authentication service.
    """
    if not settings.ws_jwt_secret:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="WS_JWT_SECRET is not configured.",
        )
    token = create_dev_ws_token(settings.ws_jwt_secret, expires_hours=24)
    return DevTokenResponse(
        token=token,
        warning="DEV ONLY — 24h expiry. Do not use in production.",
    )
