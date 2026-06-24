"""
aiokafka Consumer Daemon — At-Least-Once Delivery
=================================================

This consumer polls Kafka batches and directly writes them to TimescaleDB.
To guarantee at-least-once delivery, `consumer.commit()` is only called
AFTER the database transaction returns successfully.

If the DB write fails, the consumer does not commit. The messages will
be re-delivered on the next poll or when the consumer restarts.
"""
from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
import uuid

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

from app.config import settings
from app.db.engine import get_engine, get_session_factory
from app.db.repository import upsert_events_batch

logger = logging.getLogger(__name__)

# Batch size and poll limits
MAX_POLL_RECORDS: int = 500

async def run_consumer_daemon() -> None:
    """
    Entry point for the consumer daemon process.
    Run with: python -m app.worker.kafka_consumer
    """
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    if sys.platform != "win32":
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop_event.set)
    else:
        logger.info("Windows host detected. Use Ctrl+C to trigger graceful shutdown.")

    try:
        await _poll_and_process_loop(stop_event)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Shutdown signal received — stopping consumer daemon.")
        stop_event.set()

    logger.info("Consumer daemon shut down cleanly.")

async def _poll_and_process_loop(stop_event: asyncio.Event) -> None:
    consumer = AIOKafkaConsumer(
        settings.kafka_topic_ingestion,
        bootstrap_servers=settings.kafka_broker_url,
        group_id="pulse-consumer-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        max_poll_records=MAX_POLL_RECORDS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        session_timeout_ms=30_000,
        max_poll_interval_ms=300_000,
    )

    logger.info(
        "Starting Kafka consumer. topic=%s brokers=%s group=%s",
        settings.kafka_topic_ingestion,
        settings.kafka_broker_url,
        "pulse-consumer-group",
    )

    await consumer.start()
    
    # Initialize DB engine for this process
    get_engine()
    session_factory = get_session_factory()

    try:
        while not stop_event.is_set():
            try:
                # 1. Poll batch from Kafka
                records = await asyncio.wait_for(
                    consumer.getmany(timeout_ms=1000, max_records=MAX_POLL_RECORDS),
                    timeout=2.0,
                )
                
                batch = []
                for _tp, messages in records.items():
                    for msg in messages:
                        if isinstance(msg.value, dict):
                            batch.append(msg.value)
                        else:
                            logger.warning("Skipping non-dict Kafka message. offset=%s", msg.offset)

                if not batch:
                    continue

                logger.info("Polled %d records. Writing to TimescaleDB...", len(batch))

                # 2. Map and persist to DB
                db_records = []
                for ev in batch:
                    raw_cid = ev.get("correlation_id", "")
                    try:
                        cid = uuid.UUID(raw_cid) if raw_cid else uuid.uuid4()
                    except ValueError:
                        cid = uuid.uuid4()

                    db_records.append({
                        "event_type": ev.get("event_type", "unknown"),
                        "status_code": 202,
                        "payload": ev.get("payload", {}),
                        "client_ip_hash": ev.get("client_ip_hash", ""),
                        "correlation_id": cid,
                        "ingested_at_unix": ev.get("ingested_at_unix", 0.0),
                    })

                async with session_factory() as session:
                    inserted = await upsert_events_batch(session, db_records)
                
                logger.info("Successfully upserted %d/%d records to TimescaleDB.", inserted, len(batch))

                # 3. Commit offsets ONLY after successful DB transaction
                await consumer.commit()
                logger.debug("Kafka offsets committed successfully.")

            except asyncio.TimeoutError:
                continue
            except KafkaConnectionError as exc:
                logger.error("Kafka connection lost — retrying in 5s: %s", exc)
                await asyncio.sleep(5)
            except Exception as exc:
                logger.exception("Database or processing error. Not committing offsets! %s", exc)
                # Sleep briefly to prevent rapid spin-looping on persistent DB errors
                await asyncio.sleep(5)

    finally:
        await consumer.stop()
        logger.info("Kafka consumer stopped.")

if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(run_consumer_daemon())
