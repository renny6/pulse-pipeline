"""
Celery Worker Tasks — Persistence Pipeline
==========================================
[MANDATE — master_system_mandates.md §3 & §4]
[MANDATE — master_solution_blueprint.md §2 & §3]
[MANDATE — system_hurdles_and_guardrails.md Challenge 7, 9, 10]
[MANDATE — operations_and_observability_mandates.md §1 — Correlation IDs]

TASK DESIGN
-----------
ONE task is defined here: `persist_events_batch`.

It receives a LIST of event envelopes (the micro-batch assembled by the
Kafka consumer daemon), and persists them in a single multi-row INSERT.

Why batch tasks rather than single-event tasks?
  - Single-row task dispatch at 5,000 RPS = 5,000 Celery task objects/s
    saturating the Redis broker queue.
  - Batch tasks amortise per-task overhead across N records.
  - The micro-batch size (default 500 rows or 2s window) is controlled in
    kafka_consumer.py, NOT here — the task is agnostic to batch assembly.

CORRELATION ID PROPAGATION
--------------------------
[MANDATE — operations_and_observability_mandates.md §1 point 3]
'The X-Correlation-ID must be extracted by the Celery worker and injected
into its thread-local logging context so every background process log maps
back to the origin request.'

Every task log below includes `correlation_id` from the first record in the
batch. For full per-event tracing, individual correlation_ids are visible in
the DB rows themselves.

TASK SAFETY
-----------
SoftTimeLimitExceeded is caught explicitly — it gives the task a brief window
to route the failed batch to the DLQ before the hard SIGKILL arrives at 30s.
"""
from __future__ import annotations

import asyncio
import logging
import threading
import uuid
from typing import Any

from celery.exceptions import SoftTimeLimitExceeded

from app.db.engine import get_engine, get_session_factory
from app.db.repository import insert_dead_letter, upsert_events_batch
from app.worker.celery_app import celery_app

logger = logging.getLogger(__name__)


# ==============================================================================
# PERSISTENT WORKER EVENT LOOP
# ==============================================================================
# ROOT CAUSE OF RuntimeError: Event loop is closed
# -------------------------------------------------
# asyncio.run() creates a fresh event loop, runs the coroutine to completion,
# then IMMEDIATELY calls loop.close(). SQLAlchemy's asyncpg connection pool
# registers weakref finalizer callbacks that are designed to run cleanup I/O
# when the pool is garbage-collected. These callbacks fire AFTER asyncio.run()
# has already closed the loop, causing:
#
#     RuntimeError: Event loop is closed
#
# This happens on every GC cycle, flooding logs and risking descriptor leaks.
#
# THE FIX: Persistent loop per worker process
# -------------------------------------------
# Create ONE event loop when the first task runs in a given worker process,
# then REUSE it for every subsequent task. The pool's cleanup hooks always
# have a live event loop available until the worker process itself exits.
#
# This is the established pattern for Celery + async SQLAlchemy and is safe
# because each Celery prefork child is a single-threaded process — there is
# no cross-thread event loop sharing.
# ==============================================================================

_worker_loop: asyncio.AbstractEventLoop | None = None
_loop_lock = threading.Lock()  # Guards _worker_loop across Celery's gevent/eventlet


def _get_worker_loop() -> asyncio.AbstractEventLoop:
    """
    Return the persistent event loop for this worker process.

    Creates a new loop on first call, or recreates one if the previous loop
    was somehow closed (e.g., after a warm restart).
    """
    global _worker_loop
    with _loop_lock:
        if _worker_loop is None or _worker_loop.is_closed():
            _worker_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(_worker_loop)
            logger.debug("[worker] New persistent event loop created for this process.")
        return _worker_loop


def reset_worker_loop() -> None:
    """
    Close and discard the current persistent loop.

    Called by the worker_process_init signal in celery_app.py immediately
    after Celery forks a new child process. The forked child inherits the
    parent's loop object (which is already running in the parent) — using
    that inherited loop in the child would cause cross-process corruption.
    Resetting it here forces the child to build its own clean loop on the
    first task execution.
    """
    global _worker_loop
    with _loop_lock:
        if _worker_loop is not None:
            if not _worker_loop.is_closed():
                _worker_loop.close()
            _worker_loop = None
            logger.debug("[worker] Persistent event loop reset after fork.")


def _run_async(coro: Any) -> Any:
    """
    Run an async coroutine synchronously using the persistent worker loop.

    DELIBERATELY avoids asyncio.run() — see module-level comment above.
    Uses loop.run_until_complete() which runs the coroutine and returns
    WITHOUT closing the loop, keeping asyncpg pool hooks functional.
    """
    return _get_worker_loop().run_until_complete(coro)


# ==============================================================================
# TASK: persist_events_batch
# ==============================================================================

@celery_app.task(
    name="pulse.persist_events_batch",
    # [MANDATE — C3] Hard execution ceilings — override app defaults per-task
    # for fine-grained control. These match the app-level defaults.
    soft_time_limit=25,
    time_limit=30,
    # [MANDATE — C4] Late ack — message not acknowledged until task completes
    acks_late=True,
    # Retry configuration: 3 retries with 5s countdown, exponential factor
    max_retries=3,
    default_retry_delay=5,
    bind=True,  # Access self for retry/context
)
def persist_events_batch(self, event_batch: list[dict]) -> dict:
    """
    Persist a micro-batch of events to TimescaleDB.

    Args:
        event_batch: List of KafkaEventEnvelope dicts. Expected keys:
                     correlation_id, event_type, payload,
                     client_ip_hash, ingested_at_unix.

    Returns:
        Dict with inserted/duplicate counts for task result logging.

    [MANDATE — Challenge 7] ON CONFLICT DO NOTHING in upsert_events_batch
    guarantees idempotency on Kafka duplicate deliveries.
    [MANDATE — Challenge 10] SoftTimeLimitExceeded caught → DLQ routing.
    [MANDATE — Challenge 9] worker_max_tasks_per_child set in celery_app.py.
    """
    if not event_batch:
        logger.warning("[persist_events_batch] Received empty batch — skipping.")
        return {"inserted": 0, "skipped": 0}

    # [MANDATE — operations_observability §1]
    # Inject the first event's correlation_id into the log context for tracing.
    lead_correlation_id = event_batch[0].get("correlation_id", "N/A")
    task_log = logging.LoggerAdapter(
        logger,
        {"correlation_id": lead_correlation_id},
    )

    task_log.info(
        "[persist_events_batch] START batch_size=%d task_id=%s",
        len(event_batch),
        self.request.id,
    )

    try:
        result = _run_async(_persist(event_batch, task_log))
        task_log.info(
            "[persist_events_batch] DONE inserted=%d task_id=%s",
            result["inserted"],
            self.request.id,
        )
        return result

    except SoftTimeLimitExceeded:
        # [MANDATE — Challenge 10] Graceful cleanup before SIGKILL
        task_log.error(
            "[persist_events_batch] SOFT TIME LIMIT exceeded — routing batch "
            "to DLQ. task_id=%s batch_size=%d",
            self.request.id,
            len(event_batch),
        )
        _route_batch_to_dlq(event_batch, "SoftTimeLimitExceeded")
        # Re-raise so Celery marks the task as failed (not retry)
        raise

    except Exception as exc:
        task_log.exception(
            "[persist_events_batch] FAILED attempt=%d/%d task_id=%s error=%s",
            self.request.retries + 1,
            self.max_retries + 1,
            self.request.id,
            exc,
        )
        if self.request.retries < self.max_retries:
            # Exponential backoff: 5s, 10s, 20s
            countdown = self.default_retry_delay * (2 ** self.request.retries)
            raise self.retry(exc=exc, countdown=countdown)

        # Final retry exhausted — route to DLQ
        task_log.error(
            "[persist_events_batch] All retries exhausted — routing to DLQ. "
            "batch_size=%d",
            len(event_batch),
        )
        _route_batch_to_dlq(event_batch, f"{type(exc).__name__}: {exc}")
        return {"inserted": 0, "skipped": len(event_batch), "dlq": True}


async def _persist(batch: list[dict], task_log: logging.LoggerAdapter) -> dict:
    """
    Async inner function: builds records list and calls the repository.
    Runs via loop.run_until_complete() from _run_async — the loop is NOT
    closed after this returns, keeping asyncpg pool hooks functional.
    """
    # Ensure engine is initialised in this process (may be first task after fork)
    get_engine()
    session_factory = get_session_factory()

    # Map envelope dicts to ORM-compatible column dicts.
    # correlation_id MUST be a native uuid.UUID — asyncpg rejects str for UUID columns.
    records = []
    for ev in batch:
        raw_cid = ev.get("correlation_id", "")
        try:
            cid = uuid.UUID(raw_cid) if raw_cid else uuid.uuid4()
        except ValueError:
            task_log.warning(
                "Invalid correlation_id '%s' — substituting uuid4 fallback.", raw_cid
            )
            cid = uuid.uuid4()

        records.append({
            "event_type": ev.get("event_type", "unknown"),
            "status_code": 202,
            "payload": ev.get("payload", {}),
            "client_ip_hash": ev.get("client_ip_hash", ""),
            "correlation_id": cid,          # uuid.UUID object, not str
            "ingested_at_unix": ev.get("ingested_at_unix", 0.0),
        })

    async with session_factory() as session:
        inserted = await upsert_events_batch(session, records)

    return {"inserted": inserted, "skipped": len(batch) - inserted}


def _route_batch_to_dlq(batch: list[dict], reason: str) -> None:
    """
    Synchronous DLQ write — used inside the SoftTimeLimitExceeded handler
    where we cannot await and must act fast before the hard SIGKILL.

    Uses the persistent worker loop (not asyncio.run()) so cleanup hooks
    on the asyncpg pool remain valid.

    [MANDATE — master_solution_blueprint.md §4]
    Failed events must NEVER be silently discarded.
    """
    try:
        _run_async(_write_all_to_dlq(batch, reason))
        # Publish to Redis PubSub for WebSocket manager (Causality Diagnostics)
        _run_async(_publish_dlq_metrics(len(batch), reason))
    except Exception as exc:  # noqa: BLE001
        logger.critical(
            "CRITICAL: DLQ write also failed — %d events permanently lost. error=%s",
            len(batch),
            exc,
        )

async def _publish_dlq_metrics(count: int, reason: str) -> None:
    """Publish DLQ error metrics to Redis for WebSocket broadcast."""
    try:
        import redis.asyncio as aioredis
        import json
        from app.config import settings
        redis = aioredis.from_url(settings.redis_url)
        await redis.publish("pulse:metrics", json.dumps({
            "type": "dlq",
            "reason": reason,
            "count": count
        }))
        await redis.close()
    except Exception as exc:
        logger.error("Failed to publish DLQ metrics to Redis: %s", exc)


async def _write_all_to_dlq(batch: list[dict], reason: str) -> None:
    """Async helper: writes every failed event envelope to the DLQ table."""
    get_engine()
    session_factory = get_session_factory()
    async with session_factory() as session:
        for envelope in batch:
            await insert_dead_letter(session, envelope, reason)
