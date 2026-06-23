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
