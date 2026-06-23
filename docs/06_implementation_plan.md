# 06 | Implementation Plan — Detailed Build Sequence

## Phase 1: Infrastructure
* `docker-compose.yaml` (Kafka, Redis, TimescaleDB).
* Networking: Isolate container traffic. Setup health checks for all services.

## Phase 2: Gateway & Global Guard
* FastAPI ingestion endpoint with `X-Correlation-ID` middleware.
* Atomic Lua script for Redis Token Bucket.
* WebSocket broadcast: Aggregate metrics over 100ms windows before emitting to client to prevent browser DOM meltdown.

## Phase 3: Message Bus & Persistence
* Kafka producer (FastAPI) & consumer (Celery).
* Database connectivity: SQLAlchemy `AsyncEngine` with connection pooling.
* Teardown: `celery.signals.worker_process_init` to cycle engine pools cleanly.

## Phase 4: API Resilience
* `pybreaker` implementation for external API handshakes.
* Tenacity: Exponential backoff + Jitter for transient `429` errors.
* DLQ Routing: Route `402/403` failures to `pulse.events.dlq` for manual replay.

## Phase 5: Dashboard Visualization
* React/Vite/TypeScript frontend.
* HTML5 Canvas rendering loop for live traffic particle streams (bypassing React DOM).
