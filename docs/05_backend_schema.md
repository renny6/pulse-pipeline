# 05 | Backend Schema — Data Model & Auth Architecture

## Database Type
* **PostgreSQL (TimescaleDB Extension)**.

## Table: `ingested_events`
* `id` (UUID, Primary Key)
* `event_type` (VARCHAR, indexed)
* `status_code` (INTEGER)
* `payload` (JSONB)
* `correlation_id` (UUID, unique business key for idempotency)
* `created_at` (TIMESTAMPTZ, Time-Series index)

## Table: `dead_letter_queue` (Primary Resilience Persistence)
* `id` (UUID, Primary Key)
* `failed_payload` (JSONB)
* `error_reason` (TEXT, captures API exception context)
* `attempted_at` (TIMESTAMPTZ)
* `resolved` (BOOLEAN, default `FALSE`)

## Relationships & Indexes
* **B-Tree Index** on `correlation_id` for idempotency (`ON CONFLICT`).
* **Time-Series Index** on `created_at` for UI dashboard performance.

## Security
* **Data Masking:** Sensitive fields (e.g., `client_ip`) must be hashed or masked before being written to long-term storage or Kafka logs.
