# 🌌 PROJECT OMNIBUS: PULSE DISTRIBUTED INGESTION ENGINE

## 🎯 AI Agent Directive
You are the lead architect for **Pulse**. This document is the definitive source of truth. Do not hallucinate external tools; do not simplify architecture. 

## 1. VISION & ARCHITECTURE
Pulse is a high-throughput, event-driven ingestion pipeline (5,000+ RPS). It decouples API ingestion (FastAPI) from analytics (TimescaleDB) via Kafka. It is designed for environments with **zero-trust architectural standards**, assuming the internal Docker network is hostile.

## 2. CORE CONCURRENCY & STATE
* **Redis Lua Execution:** Use atomic Redis `EVAL` scripts for Token Bucket rate-limiting to prevent race conditions.
* **Global Clock Source:** Never use `datetime.utcnow()`. Strictly use `redis.call('TIME')` inside Lua scripts for chronological truth to mitigate cluster clock drift.
* **Thundering Herd Mitigation:** Use continuous fractional token math and randomized **Jitter** on retries to scatter traffic spikes.

## 3. BROKER & WORKER RESILIENCE
* **Idempotent Operations:** Kafka guarantees At-Least-Once delivery; all workers MUST enforce `ON CONFLICT DO NOTHING` or `UPSERT` using `correlation_id` as the business key.
* **Rebalancing Defense:** Decouple Kafka network polling from execution threads. Lower `max_poll_records` to prevent cluster-wide rebalancing storms.
* **Resource Limits:** Celery workers must use `worker_max_tasks_per_child` to recycle memory and prevent OOM-kills during high-volume ingestion.

## 4. SECURITY & PERIMETER
* **Payload Poisoning:** FastAPI must use `extra="forbid"` on Pydantic models. Drop unmapped `**kwargs` instantly with `HTTP 422`.
* **No Pickle:** Celery workers are strictly forbidden from using `pickle`. Use `task_serializer='json'`.
* **TLS Mandate:** PostgreSQL URIs must append `?ssl=require`. All internal network traffic is assumed insecure.
