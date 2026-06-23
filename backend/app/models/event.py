"""
Pydantic Data Models — Strict Schema Boundaries
================================================
[MANDATE — project_omnibus.md §4 / master_system_mandates.md §6]
[MANDATE — zero_trust_security.md §2]

Every model at the FastAPI ingestion boundary MUST use:
    model_config = ConfigDict(extra="forbid")

This drops any request field that is not explicitly declared in the model
with an immediate HTTP 422 Unprocessable Entity response. This is the primary
"poison pill" defence — malformed payloads designed to exploit JSON
deserializers never reach the Kafka messaging layer.

DO NOT use extra="allow" or accept raw Dict[str, Any] at the boundary.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


# ==============================================================================
# INGESTION REQUEST — the public API schema
# ==============================================================================

class IngestEventRequest(BaseModel):
    """
    The strict ingestion payload schema.

    [MANDATE] extra="forbid" — any field not listed here causes FastAPI to
    respond with HTTP 422 Unprocessable Entity BEFORE the handler runs.
    Attackers cannot slip extra kwargs through to downstream logic.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    event_type: str = Field(
        ...,
        min_length=1,
        max_length=64,
        # Alphanumeric, underscore, dot, hyphen only — no injection vectors
        pattern=r"^[a-zA-Z0-9_.\-]+$",
        description="Categorises the event (e.g. 'market_tick', 'user_action').",
        examples=["market_tick", "user_action", "system_alert"],
    )

    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary event data. Sensitive keys are masked before Kafka hand-off.",
    )


# ==============================================================================
# INGESTION RESPONSES
# ==============================================================================

class IngestAcceptedResponse(BaseModel):
    """HTTP 202 — event accepted, queued to Kafka."""

    status: str = "accepted"
    correlation_id: str = Field(..., description="X-Correlation-ID echoed back.")
    message: str = "Event accepted and queued for processing."
    remaining_tokens: int = Field(
        ..., description="Tokens remaining in the rate-limit bucket after this request."
    )


class IngestThrottledResponse(BaseModel):
    """HTTP 429 — rate limit exceeded."""

    status: str = "throttled"
    correlation_id: str
    message: str = "Rate limit exceeded. Back off and retry."
    retry_after_ms: int = Field(
        ..., description="Milliseconds until the bucket has sufficient tokens."
    )


# ==============================================================================
# KAFKA EVENT ENVELOPE — internal representation (not exposed at the API)
# ==============================================================================

class KafkaEventEnvelope(BaseModel):
    """
    The sanitised payload sent to the Kafka 'pulse.events.raw' topic.

    [MANDATE — operations_and_observability_mandates.md §2]
    Sensitive fields (client_ip) are ONE-WAY HASHED before being written here.
    The raw IP is never published to Kafka or logged to stdout.
    """

    model_config = ConfigDict(extra="forbid")

    correlation_id: str = Field(..., description="Unique tracing anchor.")
    event_type: str
    payload: dict[str, Any]

    # [MANDATE] SHA-256 hex digest of client IP — never the raw address.
    client_ip_hash: str = Field(..., description="SHA-256(client_ip). Never plaintext.")

    # [MANDATE] Timestamp sourced from Redis TIME (not datetime.utcnow()).
    # Set by the rate limiter layer, propagated here so workers don't need
    # to re-query the clock — preventing additional drift.
    ingested_at_unix: float = Field(
        ..., description="Unix timestamp (float seconds) from redis.call('TIME')."
    )


# ==============================================================================
# WEBSOCKET METRICS BATCH — emitted every 100ms to connected dashboard clients
# ==============================================================================

class WebSocketMetricsBatch(BaseModel):
    """
    Consolidated metrics window broadcast over WebSocket every 100ms.

    [MANDATE — system_hurdles_and_guardrails.md Challenge 2]
    Individual-event WebSocket messages at 5,000 RPS would saturate the
    client's network buffer. This batch aggregates all events within the
    100ms window into a single JSON packet.
    """

    accepted: int = Field(..., description="Events accepted by the rate limiter in this window.")
    blocked: int = Field(..., description="Events throttled (HTTP 429) in this window.")
    timestamp: str = Field(..., description="ISO-8601 UTC timestamp of window end.")
