# 02 | TRD — Technical Requirements Document

## Core Stack
* **Frontend:** React 18, Vite, Tailwind CSS v4, TypeScript.
* **Backend:** Python 3.10+, FastAPI, Uvicorn (ASGI).
* **Database:** TimescaleDB (PostgreSQL extension).
* **Message Bus:** Apache Kafka (with `aiokafka`).
* **Worker & Cache:** Celery, Redis (v3.2+ for Lua effects-based replication).

## Hosting & Infrastructure
* **Deployment:** Fully containerized via `docker-compose.yaml`.

## Key Libraries & Tools
* **Backend:** `SQLAlchemy` (AsyncEngine pooling), `pybreaker` (circuit breakers), `tenacity` (exponential backoff), `pydantic` (strict schema validation).

## Third-Party APIs
* **Primary Analytics API:** Free-tier Cloud API (e.g., financial market feed).
* **Error Handling & Resilience:** * **Circuit Breaker Pattern:** Wrapped external calls to protect worker threads.
    * **DLQ Routing:** Failed payloads routed to `pulse.events.dlq` for administrative replay.

## Environment Variables
* `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `DATABASE_URL_ASYNC`
* `REDIS_URL`, `KAFKA_BROKER_URL`
* `EXTERNAL_API_KEY`, `EXTERNAL_API_URL`
* `WS_JWT_SECRET`

## Hard Constraints
* **Zero Trust:** Strict Pydantic models (`extra = "forbid"`). Celery must use `task_serializer='json'`.
* **Idempotency:** All database writes must use `UPSERT` or `ON CONFLICT DO NOTHING`.
