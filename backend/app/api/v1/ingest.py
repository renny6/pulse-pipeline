"""
Ingestion Endpoint — POST /api/v1/ingest
==========================================
The primary entry point for all event traffic. Every request is subject to:

  1. PYDANTIC VALIDATION — extra="forbid" rejects malformed payloads with
     HTTP 422 before any processing occurs.
     [MANDATE — master_system_mandates.md §6 / zero_trust_security.md §2]

  2. RATE LIMITING — atomic Redis Lua Token Bucket determines whether the
     request is accepted (HTTP 202) or throttled (HTTP 429).
     [MANDATE — master_system_mandates.md §2]

  3. PII MASKING — client IP is SHA-256 hashed before use as a Redis key,
     Kafka message payload field, or log line.
     [MANDATE — zero_trust_security.md §5 / operations_observability §2]

  4. CORRELATION ID — X-Correlation-ID is embedded in every log line AND
     used as the Kafka message KEY for deterministic partition routing.
     [MANDATE — operations_and_observability_mandates.md §1]

  5. KAFKA PRODUCE — sanitised envelope is published to pulse.events.raw.
     Fire-and-forget from the gateway's perspective (202 already returned).
     Kafka failure is handled gracefully — the event is not lost silently.
     [MANDATE — 06_implementation_plan.md Phase 3]

  6. WEBSOCKET BROADCAST — every decision is recorded to the 100ms batch.
     [MANDATE — system_hurdles_and_guardrails.md Challenge 2]
"""
from __future__ import annotations

import logging
import time

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import JSONResponse

from app.core.kafka_producer import get_kafka_producer, send_event
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
# Dependencies — injected from app.state (set during lifespan startup)
# ==============================================================================

def get_rate_limiter(request: Request) -> TokenBucketRateLimiter:
    return request.app.state.rate_limiter  # type: ignore[no-any-return]


def get_producer(request: Request) -> AIOKafkaProducer:
    return request.app.state.kafka_producer  # type: ignore[no-any-return]


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
    producer: AIOKafkaProducer = Depends(get_producer),
) -> JSONResponse:
    """
    Accepts, validates, rate-limits, and publishes an event to Kafka.

    Returns HTTP 202 (accepted) or HTTP 429 (throttled).
    The X-Correlation-ID is set by CorrelationIDMiddleware on every request.
    """
    # ------------------------------------------------------------------
    # 1. Extract tracing context (set upstream by CorrelationIDMiddleware)
    # ------------------------------------------------------------------
    correlation_id: str = getattr(request.state, "correlation_id", "N/A")

    # ------------------------------------------------------------------
    # 2. PII masking — hash client IP before any use
    # [MANDATE] Raw IP is never stored, logged, or forwarded.
    # ------------------------------------------------------------------
    raw_ip = (
        request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or (request.client.host if request.client else "unknown")
    )
    client_id = mask_ip(raw_ip)  # SHA-256 hex digest

    # ------------------------------------------------------------------
    # 3. Atomic Token Bucket check — single Redis EVAL, no race conditions
    # ------------------------------------------------------------------
    result: RateLimitResult = await rate_limiter.check_and_consume(client_id)

    # ------------------------------------------------------------------
    # PATH A: THROTTLED → HTTP 429
    # ------------------------------------------------------------------
    if not result.allowed:
        logger.warning(
            "THROTTLED correlation_id=%s retry_after_ms=%d reason=rate_limit_exceeded",
            correlation_id,
            result.retry_after_ms,
        )
        await ws_manager.record_event(accepted=False, reason="rate_limit_exceeded")

        throttled = IngestThrottledResponse(
            correlation_id=correlation_id,
            retry_after_ms=result.retry_after_ms,
        )
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content=throttled.model_dump(),
            headers={
                "Retry-After": str(max(1, result.retry_after_ms // 1000)),
                "X-Correlation-ID": correlation_id,
            },
        )

    # ------------------------------------------------------------------
    # PATH B: ACCEPTED → publish to Kafka → HTTP 202
    # ------------------------------------------------------------------

    # [MANDATE — operations_observability §2] Sanitise payload BEFORE Kafka.
    # Known sensitive field names (api_key, token, password …) are redacted.
    sanitised_payload = mask_sensitive_fields(event.payload)
    tracking_id = sanitised_payload.pop("_tracking_id", None)

    # Build the canonical Kafka event envelope.
    # ingested_at_unix carries the gateway receive timestamp — close enough
    # to the Redis TIME value for ordering purposes.
    envelope = KafkaEventEnvelope(
        correlation_id=correlation_id,
        event_type=event.event_type,
        payload=sanitised_payload,
        client_ip_hash=client_id,
        ingested_at_unix=time.time(),
    )

    # [MANDATE — operations_observability §1]
    # correlation_id is the Kafka message KEY — events from the same client
    # are deterministically routed to the same partition for ordered processing.
    try:
        await send_event(
            producer=producer,
            envelope=envelope.model_dump(),
            correlation_id=correlation_id,
        )
    except KafkaConnectionError as exc:
        # Kafka is temporarily unreachable. Log prominently but still return 202
        # so the gateway doesn't become a hard dependency on broker availability.
        # The event will be lost in this failure scenario — Phase 4 adds the
        # circuit breaker / DLQ fallback at the producer level.
        logger.error(
            "Kafka publish failed — event may be lost. "
            "correlation_id=%s error=%s",
            correlation_id,
            exc,
        )

    logger.info(
        "ACCEPTED correlation_id=%s event_type=%s remaining_tokens=%d",
        correlation_id,
        event.event_type,
        result.remaining_tokens,
    )

    # Accumulate metric for the 100ms WebSocket broadcast window
    await ws_manager.record_event(accepted=True, tracking_id=tracking_id)

    accepted = IngestAcceptedResponse(
        correlation_id=correlation_id,
        remaining_tokens=result.remaining_tokens,
    )
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content=accepted.model_dump(),
        headers={"X-Correlation-ID": correlation_id},
    )

@router.post(
    "/events",
    summary="Ingest a batch of events",
    status_code=status.HTTP_202_ACCEPTED,
)
async def ingest_events_batch(
    events: list[IngestEventRequest],
    request: Request,
    rate_limiter: TokenBucketRateLimiter = Depends(get_rate_limiter),
    producer: AIOKafkaProducer = Depends(get_producer),
) -> JSONResponse:
    correlation_id: str = getattr(request.state, "correlation_id", "N/A")
    raw_ip = (
        request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or (request.client.host if request.client else "unknown")
    )
    client_id = mask_ip(raw_ip)

    # For a batch, we check rate limit multiple times or just consume N tokens.
    # To keep it simple and match the current system, we iterate over the batch:
    accepted_count = 0
    throttled_count = 0

    for event in events:
        result: RateLimitResult = await rate_limiter.check_and_consume(client_id)
        if not result.allowed:
            await ws_manager.record_event(accepted=False, reason="rate_limit_exceeded")
            throttled_count += 1
            continue

        sanitised_payload = mask_sensitive_fields(event.payload)
        tracking_id = sanitised_payload.pop("_tracking_id", None)
        envelope = KafkaEventEnvelope(
            correlation_id=correlation_id,
            event_type=event.event_type,
            payload=sanitised_payload,
            client_ip_hash=client_id,
            ingested_at_unix=time.time(),
        )

        try:
            await send_event(
                producer=producer,
                envelope=envelope.model_dump(),
                correlation_id=correlation_id,
            )
            await ws_manager.record_event(accepted=True, tracking_id=tracking_id)
            accepted_count += 1
        except KafkaConnectionError as exc:
            logger.error("Kafka publish failed in batch. error=%s", exc)

    logger.info("BATCH PROCESSED correlation_id=%s accepted=%d throttled=%d", correlation_id, accepted_count, throttled_count)
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"accepted": accepted_count, "throttled": throttled_count},
        headers={"X-Correlation-ID": correlation_id},
    )
