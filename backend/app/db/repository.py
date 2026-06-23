"""
Database Repository — Micro-Batched Idempotent Writes
======================================================
[MANDATE — master_system_mandates.md §4 / master_solution_blueprint.md §3]
[MANDATE — system_hurdles_and_guardrails.md Challenge 7 & 9]

MICRO-BATCHING MANDATE
-----------------------
Under 5,000 RPS, issuing one INSERT per event would hammer TimescaleDB with
thousands of concurrent single-row transactions, triggering row-level locks,
WAL log bloat, and connection exhaustion simultaneously.

This repository accumulates records in-memory and flushes in chunks via a
single multi-row INSERT … ON CONFLICT DO NOTHING statement. Chunk size and
time windows are controlled by the caller (worker/tasks.py).

  Flush triggers (whichever comes first):
    - BATCH_SIZE records accumulated  (default 500)
    - MAX_WAIT_SECONDS elapsed        (default 2.0)

ON CONFLICT DO NOTHING (IDEMPOTENCY)
--------------------------------------
The `correlation_id` column has a UNIQUE constraint (defined in ORM models).
Any Kafka duplicate delivery re-delivering the same event hits this constraint
and is silently discarded. Metrics stay clean with zero application-level
deduplication logic.

[MANDATE — zero_trust_security.md §5]
`client_ip_hash` arrives pre-hashed from the gateway — never reconstructed.
"""
from __future__ import annotations

import logging
import uuid
from typing import Sequence

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import DeadLetterEntry, IngestionEvent

logger = logging.getLogger(__name__)

# Mandate-aligned defaults — configurable by callers for testing
BATCH_SIZE: int = 500
MAX_WAIT_SECONDS: float = 2.0


async def upsert_events_batch(
    session: AsyncSession,
    records: Sequence[dict],
) -> int:
    """
    Persist a batch of event records using a single multi-row
    INSERT … ON CONFLICT (correlation_id) DO NOTHING.

    [MANDATE — Challenge 7] ON CONFLICT DO NOTHING prevents duplicate
    Kafka deliveries from poisoning analytical metrics.

    [MANDATE — Challenge 9] Single statement for N rows eliminates
    N-1 round-trip latencies and row-level lock contention.

    Args:
        session: An open AsyncSession bound to the engine.
        records: List of dicts matching IngestionEvent column names.

    Returns:
        Number of rows actually inserted (0 means all were duplicates).
    """
    if not records:
        return 0

    # Safely cast correlation_id string into a native Python UUID object
    processed_records = []
    for record in records:
        rec = dict(record)
        if "correlation_id" in rec and rec["correlation_id"] is not None:
            cid = rec["correlation_id"]
            if isinstance(cid, str):
                try:
                    rec["correlation_id"] = uuid.UUID(cid)
                except ValueError:
                    rec["correlation_id"] = uuid.uuid4()
            elif not isinstance(cid, uuid.UUID):
                try:
                    rec["correlation_id"] = uuid.UUID(str(cid))
                except ValueError:
                    rec["correlation_id"] = uuid.uuid4()
        processed_records.append(rec)

    stmt = (
        pg_insert(IngestionEvent)
        .values(processed_records)
        .on_conflict_do_nothing(index_elements=["correlation_id"])
    )

    result = await session.execute(stmt)
    await session.commit()

    inserted = result.rowcount if result.rowcount is not None else 0
    duplicates = len(records) - inserted

    logger.info(
        "Batch upsert complete — inserted=%d, duplicates_skipped=%d",
        inserted,
        duplicates,
    )
    return inserted


async def insert_dead_letter(
    session: AsyncSession,
    failed_payload: dict,
    error_reason: str,
) -> None:
    """
    Write a single failed event to the dead_letter_queue table.

    Called by the Celery task when a hard failure occurs after retries,
    or when the external API circuit breaker is OPEN (Phase 4).

    [MANDATE — Challenge 10] Workers must not silently discard failed
    events. Route them to the DLQ for operator triage and replay.

    Args:
        session:        An open AsyncSession.
        failed_payload: The raw event envelope dict that failed processing.
        error_reason:   Exception class + message string for debugging.
    """
    entry = DeadLetterEntry(
        failed_payload=failed_payload,
        error_reason=error_reason,
    )
    session.add(entry)
    await session.commit()
    logger.warning(
        "Event routed to dead_letter_queue. reason=%s payload_keys=%s",
        error_reason[:120],
        list(failed_payload.keys()),
    )


async def health_check_db(session: AsyncSession) -> bool:
    """
    Lightweight DB connectivity check used by the readiness probe.

    Returns True if TimescaleDB is reachable, False otherwise.
    """
    try:
        await session.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        logger.error("DB health check failed: %s", exc)
        return False


async def get_aggregated_metrics(
    session: AsyncSession,
    metric_name: str,
    bucket_minutes: int,
) -> list[dict]:
    """
    Query TimescaleDB to aggregate payload values over time buckets using native
    time_bucket and cast mechanics.

    [MANDATE — Challenge 9] Uses native TimescaleDB time_bucket for high performance
    aggregation over hypertable partitions.
    """
    import datetime

    # Use datetime.timedelta as the native type mapping to PostgreSQL INTERVAL
    interval_val = datetime.timedelta(minutes=bucket_minutes)

    query = text("""
        SELECT 
            time_bucket(:bucket_interval, created_at) AS bucket,
            COUNT(*) as total_ticks,
            MIN((payload->>'value')::float) as min_value,
            MAX((payload->>'value')::float) as max_value,
            AVG((payload->>'value')::float) as avg_value
        FROM ingested_events
        WHERE event_type = :metric_name
        GROUP BY bucket
        ORDER BY bucket DESC
        LIMIT 20;
    """)

    result = await session.execute(
        query,
        {
            "bucket_interval": interval_val,
            "metric_name": metric_name,
        }
    )

    rows = result.all()
    # Map raw Row objects to dicts, formatting datetime as ISO string for JSON serialization
    payloads = []
    for row in rows:
        payloads.append({
            "bucket": row.bucket.isoformat() if row.bucket else None,
            "total_ticks": row.total_ticks,
            "min_value": row.min_value,
            "max_value": row.max_value,
            "avg_value": row.avg_value,
        })

    return payloads
