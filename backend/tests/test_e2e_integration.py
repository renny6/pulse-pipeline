"""
End-to-End Integration Smoke Test
=================================

This test verifies the complete data flow from Kafka to TimescaleDB, confirming
that the `kafka_consumer.py` successfully processes and persists batches.

HOW TO RUN WITHIN DOCKER-COMPOSE:
---------------------------------
Because this test needs to resolve `kafka` and `timescaledb` hosts exactly as
defined in your `backend/app/core/config.py`, the easiest way to run this is
from INSIDE the running API container using `pytest` and `pytest-asyncio`.

1. Ensure the containers are running:
   docker compose up -d

2. Execute the test inside the `api` container:
   docker compose exec pulse-api pip install pytest pytest-asyncio
   docker compose exec pulse-api pytest tests/test_e2e_integration.py -v -s
"""
import asyncio
import json
import uuid
import time
import pytest

from aiokafka import AIOKafkaProducer
from sqlalchemy import text

from app.config import settings
from app.db.engine import get_engine, get_session_factory

# We mark the entire module for asyncio to avoid boilerplate
pytestmark = pytest.mark.asyncio

async def test_kafka_to_timescaledb_e2e():
    """
    Smoke test for the new at-least-once consumer logic.
    1. Connects to Kafka and Database.
    2. Generates a unique test run ID.
    3. Produces 100 mock events to Kafka matching the ingestion envelope.
    4. Waits for the kafka_consumer daemon to process them.
    5. Asserts exactly 100 events were written to TimescaleDB.
    """
    # ---------------------------------------------------------
    # 1. SETUP: Database & Kafka Connections
    # ---------------------------------------------------------
    get_engine()
    session_factory = get_session_factory()
    
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_broker_url,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await producer.start()

    # We use a unique correlation_id to isolate this test's data
    # from any background traffic simulator noise.
    test_run_id = str(uuid.uuid4())
    event_count = 100

    print(f"\n[TEST] Starting E2E test run with correlation_id: {test_run_id}")

    try:
        # ---------------------------------------------------------
        # 2. PRODUCE: Send 100 events to Kafka
        # ---------------------------------------------------------
        print(f"[TEST] Producing {event_count} events to topic: {settings.kafka_topic_ingestion}...")
        for i in range(event_count):
            # Generate a truly random UUID for each event to prevent ON CONFLICT DO NOTHING from dropping them
            event_cid = str(uuid.uuid4())
            envelope = {
                "correlation_id": event_cid,
                "event_type": "e2e_smoke_test",
                "payload": {"test_run": test_run_id, "test_index": i, "data": "dummy_data"},
                "client_ip_hash": "test_hash_123",
                "ingested_at_unix": time.time(),
            }
            await producer.send_and_wait(
                topic=settings.kafka_topic_ingestion,
                value=envelope,
                key=event_cid,
            )
        print("[TEST] All events produced successfully.")

        # ---------------------------------------------------------
        # 3. SYNCHRONIZATION: Wait for consumer to process
        # ---------------------------------------------------------
        # The consumer polls every 1000ms max, and processes 500 records at once.
        # 5 seconds is plenty of time for the pipeline to flush to the database.
        wait_time = 5
        print(f"[TEST] Waiting {wait_time} seconds for kafka_consumer to persist to TimescaleDB...")
        await asyncio.sleep(wait_time)

        # ---------------------------------------------------------
        # 4. VERIFICATION: Query TimescaleDB
        # ---------------------------------------------------------
        print("[TEST] Querying TimescaleDB for verification...")
        
        async with session_factory() as session:
            # Query by the test_run_id we embedded in the payload
            query = text(
                "SELECT COUNT(*) FROM ingested_events WHERE payload->>'test_run' = :cid"
            )
            result = await session.execute(query, {"cid": test_run_id})
            persisted_count = result.scalar()

        print(f"[TEST] Found {persisted_count} events in DB.")

        # ---------------------------------------------------------
        # 5. ASSERTION
        # ---------------------------------------------------------
        assert persisted_count == event_count, (
            f"Data loss detected! Expected {event_count} events, but found {persisted_count}. "
            "Is kafka_consumer.py running and committing successfully?"
        )
        print("[TEST] Success! At-least-once delivery verified.")

    finally:
        await producer.stop()
