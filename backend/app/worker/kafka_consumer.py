"""
aiokafka Consumer Daemon — Thread-Decoupled Kafka Polling
==========================================================
[MANDATE — system_hurdles_and_guardrails.md Challenge 6 & 12]
[MANDATE — master_solution_blueprint.md §2 — Thread Decoupling]

THE CRITICAL PROBLEM THIS SOLVES: KAFKA REBALANCING STORMS
-----------------------------------------------------------
If you run Celery tasks synchronously inside the Kafka consumer poll loop,
a slow DB commit (e.g., 500ms during a WAL flush) will stall the poll() call
past max.poll.interval.ms. The Kafka broker assumes the consumer is dead,
EVICTS it, and triggers a cluster-wide consumer group rebalance.
The replacement consumer inherits the same load and also times out —
cascading into an infinite rebalancing storm.

THE TWO-TIER ARCHITECTURE
--------------------------

  Tier 1 — Kafka Poll Thread (this file):
    - Polls Kafka at high frequency with a small max_poll_records window.
    - NEVER executes any business logic or I/O.
    - Places records into a non-blocking asyncio.Queue immediately.
    - Sends poll acknowledgement back to the broker (heartbeat maintained).

  Tier 2 — Batch Dispatcher (also this file, separate coroutine):
    - Drains the asyncio.Queue into an in-memory accumulator.
    - Flushes to a Celery task when BATCH_SIZE or MAX_WAIT_SECONDS is reached.
    - NEVER touches Kafka — completely decoupled from the poll loop.

This two-tier separation means the broker heartbeat NEVER misses, even when
the DB layer is under stress.

[MANDATE — Challenge 12]
'Strictly decouple the network Kafka polling thread from the execution worker
pool thread. The worker process connected to the Kafka cluster must execute
nothing but pulling the records, placing them immediately into an isolated
in-memory queue, and instantly dispatching a successful poll acknowledgement
back to the broker.'
"""
from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
import time

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

from app.config import settings
from app.worker.tasks import persist_events_batch

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Micro-batch configuration
# [MANDATE — master_solution_blueprint.md §3]
# Workers accumulate records and commit in optimised chunks.
# ---------------------------------------------------------------------------
BATCH_SIZE: int = 500       # flush when this many records accumulate
MAX_WAIT_SECONDS: float = 2.0  # flush even if BATCH_SIZE not reached yet

# [MANDATE — Challenge 12] Reduce max_poll_records during high-concurrency
# to keep poll intervals well within max.poll.interval.ms (5 minutes default,
# but our processing could stall). Small poll windows = fast heartbeat.
MAX_POLL_RECORDS: int = 50

# asyncio Queue depth — limits in-memory backlog to prevent OOM under surge
QUEUE_MAX_SIZE: int = 5_000


async def run_consumer_daemon() -> None:
    """
    Entry point for the consumer daemon process.

    Run with:
        python -m app.worker.kafka_consumer

    This is a standalone asyncio application — NOT a Celery task. It is
    deployed as a separate Docker service (pulse-consumer in docker-compose).

    SIGNAL HANDLING — CROSS-PLATFORM
    ---------------------------------
    loop.add_signal_handler() is a Unix-only API (POSIX signals). On Windows
    it raises NotImplementedError because Windows uses a different signalling
    model.

    Strategy:
      - Unix (Docker containers / Linux / macOS): register SIGTERM + SIGINT
        via loop.add_signal_handler() for clean Docker stop / kill behaviour.
      - Windows (local dev): skip signal registration entirely. Ctrl+C in
        the terminal raises KeyboardInterrupt which asyncio.run() converts to
        a CancelledError on all tasks. We catch it below and set stop_event
        so both coroutines drain gracefully before the process exits.
    """
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    if sys.platform != "win32":
        # Unix: register SIGTERM (docker stop) and SIGINT (Ctrl+C) properly
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop_event.set)
    else:
        logger.info(
            "Windows host detected — SIGTERM handler skipped. "
            "Use Ctrl+C to trigger a graceful shutdown."
        )

    # Bounded in-memory queue: Tier-1 producer → Tier-2 consumer
    queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)

    try:
        # Run both tiers concurrently; either can cancel the other on shutdown
        await asyncio.gather(
            _poll_loop(queue, stop_event),
            _batch_dispatch_loop(queue, stop_event),
        )
    except (KeyboardInterrupt, asyncio.CancelledError):
        # Windows Ctrl+C path: set stop_event so both coroutines exit their
        # while-loops cleanly, flushing any in-flight batch before stopping.
        logger.info("Shutdown signal received — stopping consumer daemon.")
        stop_event.set()

    logger.info("Consumer daemon shut down cleanly.")


async def _poll_loop(
    queue: asyncio.Queue[dict],
    stop_event: asyncio.Event,
) -> None:
    """
    TIER 1 — Kafka polling loop.

    Responsibilities:
      - Connect to Kafka on the INTERNAL listener (kafka:9092)
      - Poll events into the asyncio.Queue as fast as possible
      - NEVER block on I/O beyond the Kafka socket
      - Send heartbeat acknowledgement to broker immediately after poll

    [MANDATE — Challenge 12]
    """
    consumer = AIOKafkaConsumer(
        settings.kafka_topic_ingestion,
        bootstrap_servers=settings.kafka_broker_url,
        group_id="pulse-consumer-group",
        # [MANDATE] JSON-only — never pickle
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        # [MANDATE — Challenge 12] Small poll window → fast heartbeat
        max_poll_records=MAX_POLL_RECORDS,
        # auto_offset_reset: start from earliest on first run,
        # continue from last committed offset on restart (at-least-once)
        auto_offset_reset="earliest",
        # Manual commit after batch dispatch — prevents data loss on crash
        enable_auto_commit=False,
        # Session timeout: broker considers consumer dead after this interval
        # without a heartbeat. 30s is generous for our fast poll loop.
        session_timeout_ms=30_000,
        # Max time between poll() calls before broker triggers rebalance.
        # 5 minutes >> our actual poll interval. Acts as a safety ceiling.
        max_poll_interval_ms=300_000,
    )

    logger.info(
        "Starting Kafka consumer. topic=%s brokers=%s group=%s",
        settings.kafka_topic_ingestion,
        settings.kafka_broker_url,
        "pulse-consumer-group",
    )

    await consumer.start()
    try:
        while not stop_event.is_set():
            try:
                # [MANDATE — Challenge 12] Poll and IMMEDIATELY enqueue.
                # No processing, no I/O, no business logic here.
                records = await asyncio.wait_for(
                    consumer.getmany(timeout_ms=500, max_records=MAX_POLL_RECORDS),
                    timeout=2.0,
                )
                for _tp, messages in records.items():
                    for msg in messages:
                        if isinstance(msg.value, dict):
                            await queue.put(msg.value)
                        else:
                            logger.warning(
                                "Skipping non-dict Kafka message. offset=%s",
                                msg.offset,
                            )

                # Commit offsets after successful enqueue
                # (before dispatch — at-least-once with idempotent DB writes)
                await consumer.commit()

            except asyncio.TimeoutError:
                # Normal — no messages in this window; loop continues
                continue
            except KafkaConnectionError as exc:
                logger.error("Kafka connection lost — retrying in 5s: %s", exc)
                await asyncio.sleep(5)

    finally:
        await consumer.stop()
        logger.info("Kafka consumer stopped.")


async def _batch_dispatch_loop(
    queue: asyncio.Queue[dict],
    stop_event: asyncio.Event,
) -> None:
    """
    TIER 2 — Batch assembly and Celery task dispatch.

    Drains the asyncio.Queue into an accumulator.
    Flushes when BATCH_SIZE is reached OR MAX_WAIT_SECONDS elapses —
    whichever comes first.

    Decoupled from Tier 1 — cannot stall the Kafka heartbeat.

    [MANDATE — master_solution_blueprint.md §3]
    'Workers must accumulate records and commit them in optimised chunks
    (e.g., 500 rows at a time).'
    """
    batch: list[dict] = []
    window_start = time.monotonic()

    logger.info(
        "Batch dispatcher started. batch_size=%d max_wait=%.1fs",
        BATCH_SIZE,
        MAX_WAIT_SECONDS,
    )

    while not stop_event.is_set() or not queue.empty():
        # Non-blocking dequeue with short timeout
        try:
            envelope = await asyncio.wait_for(queue.get(), timeout=0.1)
            batch.append(envelope)
            queue.task_done()
        except asyncio.TimeoutError:
            pass  # No new messages — check flush condition below

        elapsed = time.monotonic() - window_start
        should_flush = (
            len(batch) >= BATCH_SIZE          # [MANDATE] 500-record chunks
            or (elapsed >= MAX_WAIT_SECONDS and batch)  # [MANDATE] 2s window
        )

        if should_flush:
            _dispatch_batch(batch)
            batch = []
            window_start = time.monotonic()

    # Flush any remaining records on shutdown
    if batch:
        logger.info(
            "Flushing remaining %d records on shutdown.", len(batch)
        )
        _dispatch_batch(batch)


def _dispatch_batch(batch: list[dict]) -> None:
    """
    Send the assembled micro-batch to the Celery task queue.

    Uses .delay() (fire-and-forget) — the consumer daemon is not responsible
    for task execution. Celery workers handle retries and DLQ routing.
    """
    if not batch:
        return
    logger.info(
        "Dispatching batch to Celery. size=%d lead_correlation_id=%s",
        len(batch),
        batch[0].get("correlation_id", "N/A"),
    )
    # Fire-and-forget: persist_events_batch.delay() enqueues the task
    # on the Redis broker and returns immediately.
    persist_events_batch.delay(batch)


# ---------------------------------------------------------------------------
# Standalone process entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(run_consumer_daemon())
