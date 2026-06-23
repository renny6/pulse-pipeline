"""
aiokafka Async Producer — Kafka Message Bus Integration
========================================================
[MANDATE — system_hurdles_and_guardrails.md Challenge 5 & 6]
[MANDATE — operations_and_observability_mandates.md §1]

PRODUCER DESIGN
---------------
One AIOKafkaProducer instance is created at FastAPI startup and shared across
all request handler coroutines via app.state. This avoids the overhead of
creating a new connection per request.

KEY DESIGN DECISIONS:

  [K1] correlation_id as the Kafka message KEY
       Events from the same client are deterministically routed to the
       same partition, guaranteeing ordered processing by Celery workers.

  [K2] JSON-serialised KafkaEventEnvelope as the VALUE
       Workers deserialise with the same Pydantic model — no Pickle, no
       schema drift, no RCE vectors.

  [K3] acks='all' (wait for ALL in-sync replicas to acknowledge)
       Prevents data loss on broker leader failure. Slightly increases
       producer latency but guarantees durability.

  [K4] enable_idempotence=True
       Broker-side exactly-once delivery de-duplication at the network level.
       Application-level ON CONFLICT DO NOTHING in the DB handles the rest.

  [K5] compression_type='gzip'
       Compresses batches before wire transmission. Reduces Kafka disk usage
       and network bandwidth significantly at high RPS.

  [K6] INTERNAL listener (kafka:9092) inside Docker
       The EXTERNAL listener (localhost:9094) is only for host-side tooling.
       [MANDATE — Challenge 5 / docker-compose.yaml dual-listener config]
"""
from __future__ import annotations

import json
import logging

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from app.config import settings

logger = logging.getLogger(__name__)

_producer: AIOKafkaProducer | None = None


async def init_kafka_producer() -> AIOKafkaProducer:
    """
    Create, start, and return the module-level AIOKafkaProducer.
    Called once from FastAPI lifespan startup.
    """
    global _producer

    _producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_broker_url,
        # [K1] Serialise key as UTF-8 bytes (correlation_id string)
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        # [K2] Serialise value as JSON bytes
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # [K3] Durability: wait for all in-sync replica acknowledgements
        acks="all",
        # [K4] Broker-side idempotent delivery (prevents network-level dups)
        enable_idempotence=True,
        # [K5] Compress batch payloads over the wire
        compression_type="gzip",
        # Safety timeouts — fail fast rather than hanging request handlers
        request_timeout_ms=5_000,
        retry_backoff_ms=100,
    )

    await _producer.start()
    logger.info("✓ Kafka producer started. brokers=%s", settings.kafka_broker_url)
    return _producer


async def close_kafka_producer() -> None:
    """Stop and drain the producer. Called from FastAPI lifespan shutdown."""
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None
        logger.info("Kafka producer stopped and drained.")


def get_kafka_producer() -> AIOKafkaProducer:
    """
    Return the module-level producer.
    Used as a FastAPI dependency in ingest.py.
    """
    if _producer is None:
        raise RuntimeError(
            "Kafka producer not initialised. "
            "Ensure init_kafka_producer() is called in the app lifespan."
        )
    return _producer


async def send_event(
    producer: AIOKafkaProducer,
    envelope: dict,
    correlation_id: str,
) -> None:
    """
    Send a single event envelope to the primary ingestion topic.

    The message is fire-and-forget from the gateway's perspective (HTTP 202
    has already been returned). The producer's internal retry + acks='all'
    guarantee delivery to Kafka before the send future resolves.

    [MANDATE — operations_observability §1]
    correlation_id is used as the Kafka message key so every log line
    in the Celery worker can reference back to the origin request.

    Args:
        producer:       The shared AIOKafkaProducer instance.
        envelope:       Dict representation of KafkaEventEnvelope.
        correlation_id: The X-Correlation-ID from the gateway middleware.
    """
    try:
        await producer.send_and_wait(
            settings.kafka_topic_ingestion,
            key=correlation_id,
            value=envelope,
        )
        logger.debug(
            "Event published. topic=%s key=%s",
            settings.kafka_topic_ingestion,
            correlation_id,
        )
    except KafkaConnectionError as exc:
        # Log and re-raise — the gateway should return 503 if Kafka is down.
        # For Phase 3 the caller (ingest.py) handles the fallback.
        logger.error(
            "Kafka send failed. correlation_id=%s error=%s",
            correlation_id,
            exc,
        )
        raise


async def send_to_dlq(
    producer: AIOKafkaProducer,
    payload: dict,
    reason: str,
) -> None:
    """
    Route an unprocessable event to the Dead Letter Queue topic.

    [MANDATE — master_system_mandates.md §5 / master_solution_blueprint.md §4]
    Workers that exceed retries or hit hard failures publish here instead of
    discarding the event. A separate admin process or Phase 4 task replays DLQ
    entries when the downstream system recovers.
    """
    dlq_envelope = {"payload": payload, "error_reason": reason}
    try:
        await producer.send_and_wait(
            settings.kafka_topic_dlq,
            key=payload.get("correlation_id", "unknown"),
            value=dlq_envelope,
        )
        logger.warning(
            "Event routed to DLQ. topic=%s reason=%s",
            settings.kafka_topic_dlq,
            reason[:120],
        )
    except Exception as exc:  # noqa: BLE001
        logger.critical(
            "CRITICAL: DLQ send also failed — event permanently lost. "
            "payload_keys=%s kafka_error=%s",
            list(payload.keys()),
            exc,
        )
