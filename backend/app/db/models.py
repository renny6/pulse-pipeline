"""
SQLAlchemy ORM Models — TimescaleDB Schema
==========================================
[MANDATE — docs/05_backend_schema.md]
[MANDATE — master_system_mandates.md §4 / zero_trust_security.md §5]

Tables defined here mirror the schema bootstrapped by
infrastructure/init-db.sql in Phase 1 (which used IF NOT EXISTS).
SQLAlchemy metadata is used by the repository layer for inserts — Alembic
handles all structural migrations.

DATA MASKING GUARANTEE
----------------------
`client_ip_hash` in IngestionEvent stores the SHA-256 digest of the raw IP,
never the plaintext address. This field arrives pre-hashed from the FastAPI
gateway (app/core/security.py::mask_ip) and is never reconstructed.

IDEMPOTENCY KEY
--------------
`correlation_id` has a UNIQUE constraint — the PRIMARY guard against
Kafka's at-least-once duplicate delivery. Any re-delivered message with the
same correlation_id hits ON CONFLICT DO NOTHING in the repository layer and
is silently discarded, keeping metrics clean.

TIMESCALEDB HYPERTABLE
----------------------
`ingested_events` is converted to a TimescaleDB hypertable by init-db.sql
(create_hypertable on 'created_at'). SQLAlchemy treats it as a plain table
for query purposes — no special ORM changes needed.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


# ==============================================================================
# TABLE: ingested_events
# ==============================================================================

class IngestionEvent(Base):
    """
    Persisted record for every successfully rate-limited event.

    [MANDATE — 05_backend_schema.md]
    correlation_id is the idempotency key — UNIQUE constraint + ON CONFLICT
    DO NOTHING in all INSERT operations guards against Kafka duplicate delivery.
    """
    __tablename__ = "ingested_events"
    __table_args__ = (
        UniqueConstraint("correlation_id", name="uq_ingested_events_correlation_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    status_code: Mapped[int] = mapped_column(Integer, nullable=False, default=202)

    # JSONB for flexible payload storage with native PostgreSQL JSON indexing
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # [MANDATE] SHA-256 of client IP — never plaintext
    client_ip_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    # Idempotency key: correlates gateway log → Kafka event → DB row
    correlation_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)

    # ingested_at_unix: the Redis TIME timestamp from the gateway Lua script,
    # preserved through the pipeline to guarantee clock-drift-free ordering.
    ingested_at_unix: Mapped[float] = mapped_column(nullable=False)

    # created_at: TimescaleDB hypertable partition key.
    # Set at insertion time using the DB server clock (timezone-aware).
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        index=True,
    )


# ==============================================================================
# TABLE: dead_letter_queue
# ==============================================================================

class DeadLetterEntry(Base):
    """
    Persists Kafka events that could not be processed after all retries.

    [MANDATE — master_system_mandates.md §5 / master_solution_blueprint.md §4]
    [MANDATE — system_hurdles_and_guardrails.md Challenge 10]

    Workers route here instead of raising a fatal exception when:
      - External API circuit breaker is OPEN (Phase 4)
      - task hard time_limit is exceeded (zombie task protection)
      - Validation / structural error not caught at the gateway

    `resolved` defaults to False. An admin UI or replay script queries
    WHERE resolved = FALSE for manual triage and re-injection.
    """
    __tablename__ = "dead_letter_queue"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    failed_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)

    # Captures the exception class + message for operator debugging
    error_reason: Mapped[str] = mapped_column(Text, nullable=False)

    attempted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    resolved: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
