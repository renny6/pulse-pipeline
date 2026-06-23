"""
Ingestion Endpoint — POST /api/v1/ingest
==========================================
The primary entry point for all event traffic. Every request passing through
this endpoint is subject to:

  1. PYDANTIC VALIDATION — extra="forbid" rejects malformed payloads with
     HTTP 422 before any processing occurs.
     [MANDATE — master_system_mandates.md §6 / zero_trust_security.md §2]

  2. RATE LIMITING — atomic Redis Lua Token Bucket determines whether the
     request is accepted (HTTP 202) or throttled (HTTP 429).
     [MANDATE — master_system_mandates.md §2 / project_omnibus.md §2]

  3. PII MASKING — client IP is SHA-256 hashed before use as a Redis key,
     Kafka payload field, or log line.
     [MANDATE — zero_trust_security.md §5 / operations_observability §2]

  4. CORRELATION ID — X-Correlation-ID is embedded in every log line and
     will be carried through the Kafka → Celery pipeline in Phase 3.
     [MANDATE — operations_and_observability_mandates.md §1]

  5. WEBSOCKET BROADCAST — every decision (accepted / blocked) is recorded
     to the WebSocket manager for the 100ms batch emission.
     [MANDATE — system_hurdles_and_guardrails.md Challenge 2]

  6. KAFKA PRODUCER STUB — the sanitised KafkaEventEnvelope is ready for
     Phase 3. A TODO marker shows exactly where aiokafka.send() inserts.
     [MANDATE — project_omnibus.md §3 / 06_implementation_plan.md Phase 3]
"""
from __future__ import annotations

import logging
import time

from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import JSONResponse

from app.core.rate_limiter import RateLimitResult, TokenBucketRateLimiter
from app.core.security import mask_ip, mask_sensitive_fields
from app.models.event import (
    IngestAcceptedResponse,
    IngestEventRequest,
    IngestThrottledResponse,
    KafkaEventEnvelope,
)
from app.ws.manager import ws_manager

logger = logging.getLogger(__name__)

router = APIRouter()


# ==============================================================================
# Dependency — injects the rate limiter from app.state
# ==============================================================================

def get_rate_limiter(request: Request) -> TokenBucketRateLimiter:
    """
    FastAPI dependency that retrieves the singleton TokenBucketRateLimiter
    stored on app.state during the lifespan startup.
    """
    return request.app.state.rate_limiter  # type: ignore[no-any-return]


# ==============================================================================
# Ingestion endpoint
# ==============================================================================

@router.post(
    "/ingest",
    summary="Ingest a single event",
    response_description="Event accepted (202) or throttled (429)",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        202: {"model": IngestAcceptedResponse, "description": "Event queued to Kafka."},
        422: {"description": "Malformed payload — field validation failed."},
        429: {"model": IngestThrottledResponse, "description": "Rate limit exceeded."},
    },
)
async def ingest_event(
    event: IngestEventRequest,
    request: Request,
    rate_limiter: TokenBucketRateLimiter = Depends(get_rate_limiter),
) -> JSONResponse:
    """
    Accepts an event, applies the global Token Bucket rate limit, and queues
    it to Kafka. Returns HTTP 202 on success, HTTP 429 on throttle.

    The X-Correlation-ID for this request is available in request.state
    and in all log lines below.
    """
    # -------------------------------------------------------------------------
    # Extract tracing context set by CorrelationIDMiddleware
    # -------------------------------------------------------------------------
    correlation_id: str = getattr(request.state, "correlation_id", "N/A")

    # -------------------------------------------------------------------------
    # [MANDATE] PII masking — hash the client IP for rate-limit key and logs.
    # The raw IP is never stored or forwarded beyond this point.
    # -------------------------------------------------------------------------
    raw_ip = (
        request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or (request.client.host if request.client else "unknown")
    )
    client_id = mask_ip(raw_ip)  # SHA-256 hex digest

    # -------------------------------------------------------------------------
    # [MANDATE] Atomic Token Bucket check — single Redis EVAL, no race conditions
    # -------------------------------------------------------------------------
    result: RateLimitResult = await rate_limiter.check_and_consume(client_id)

    # -------------------------------------------------------------------------
    # PATH: THROTTLED (HTTP 429)
    # -------------------------------------------------------------------------
    if not result.allowed:
        logger.warning(
            "THROTTLED correlation_id=%s client_id=%s retry_after_ms=%d",
            correlation_id,
            client_id,
            result.retry_after_ms,
        )

        # [MANDATE] Record blocked event for 100ms WebSocket batch
        await ws_manager.record_event(accepted=False)

        throttled = IngestThrottledResponse(
            correlation_id=correlation_id,
            retry_after_ms=result.retry_after_ms,
        )
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content=throttled.model_dump(),
            headers={
                "Retry-After": str(result.retry_after_ms // 1000 or 1),
                "X-Correlation-ID": correlation_id,
            },
        )

    # -------------------------------------------------------------------------
    # PATH: ACCEPTED (HTTP 202)
    # -------------------------------------------------------------------------

    # [MANDATE — operations_observability §2] Sanitise payload BEFORE any
    # Kafka hand-off or logging. Known sensitive field names are redacted.
    sanitised_payload = mask_sensitive_fields(event.payload)

    # Build the Kafka event envelope (ready for Phase 3 aiokafka producer).
    # ingested_at_unix uses Python time.time() here as a close approximation.
    # In Phase 3, the actual Redis TIME value from the Lua script result will
    # be passed through so the clock source remains Redis (not the container).
    envelope = KafkaEventEnvelope(
        correlation_id=correlation_id,
        event_type=event.event_type,
        payload=sanitised_payload,
        client_ip_hash=client_id,
        ingested_at_unix=time.time(),
    )

    # -------------------------------------------------------------------------
    # [PHASE 3 TODO] Kafka producer hand-off
    # Replace this comment block with the aiokafka send call:
    #
    #   await kafka_producer.send(
    #       settings.kafka_topic_ingestion,
    #       key=correlation_id.encode(),
    #       value=envelope.model_dump_json().encode(),
    #   )
    #
    # The correlation_id is used as the Kafka message KEY so that events from
    # the same client are deterministically routed to the same partition,
    # enabling ordered processing by Celery workers.
    # -------------------------------------------------------------------------

    logger.info(
        "ACCEPTED correlation_id=%s event_type=%s remaining_tokens=%d",
        correlation_id,
        event.event_type,
        result.remaining_tokens,
    )

    # [MANDATE] Record accepted event for 100ms WebSocket batch
    await ws_manager.record_event(accepted=True)

    accepted = IngestAcceptedResponse(
        correlation_id=correlation_id,
        remaining_tokens=result.remaining_tokens,
    )
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content=accepted.model_dump(),
        headers={"X-Correlation-ID": correlation_id},
    )
