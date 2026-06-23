"""
Schema Migration Script — Phase 3 Sync
=======================================
Fixes the two errors caused by the local TimescaleDB schema being out of
sync with the Phase 3 SQLAlchemy models:

  1. ProgrammingError: column "ingested_at_unix" does not exist
     → ALTER TABLE ingested_events ADD COLUMN IF NOT EXISTS ingested_at_unix

  2. UndefinedTableError: dead_letter_queue
     → CREATE TABLE IF NOT EXISTS dead_letter_queue (...)

RUN FROM THE backend/ DIRECTORY:
    python scripts/migrate.py

Safe to run multiple times — all statements use IF NOT EXISTS / IF EXISTS
guards so re-running never errors or corrupts data.
"""
from __future__ import annotations

import asyncio
import logging
import sys

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# Ensure the app package is importable when running from backend/
sys.path.insert(0, ".")

from app.config import settings
from app.db.models import Base

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQL statements — each is idempotent (safe to re-run)
# ---------------------------------------------------------------------------

# 1. Add missing column to existing ingested_events table.
#    DOUBLE PRECISION matches SQLAlchemy Float (Python float).
#    DEFAULT 0.0 back-fills existing rows so the NOT NULL constraint is met.
ADD_INGESTED_AT_UNIX = text("""
    ALTER TABLE ingested_events
    ADD COLUMN IF NOT EXISTS ingested_at_unix DOUBLE PRECISION NOT NULL DEFAULT 0.0;
""")

# 2. Add client_ip_hash if also missing (blank string default for existing rows)
ADD_CLIENT_IP_HASH = text("""
    ALTER TABLE ingested_events
    ADD COLUMN IF NOT EXISTS client_ip_hash VARCHAR(64) NOT NULL DEFAULT '';
""")

# 3. Ensure the unique constraint on correlation_id exists (idempotent)
ADD_CORRELATION_UNIQUE = text("""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'uq_ingested_events_correlation_id'
        ) THEN
            ALTER TABLE ingested_events
            ADD CONSTRAINT uq_ingested_events_correlation_id
            UNIQUE (correlation_id);
        END IF;
    END
    $$;
""")


async def run_migration() -> None:
    db_url = settings.database_url_async
    if not db_url:
        logger.error(
            "DATABASE_URL_ASYNC is not set. "
            "Check backend/.env or the root .env file."
        )
        sys.exit(1)

    logger.info("Connecting to: %s", db_url.split("@")[-1])  # log host only, not password

    engine = create_async_engine(db_url, echo=False)

    async with engine.begin() as conn:

        # ------------------------------------------------------------------
        # Step 1 — Alter ingested_events: add missing columns
        # ------------------------------------------------------------------
        logger.info("Step 1/3: Adding missing columns to ingested_events …")
        await conn.execute(ADD_INGESTED_AT_UNIX)
        logger.info("  ✓ ingested_at_unix column ensured.")

        await conn.execute(ADD_CLIENT_IP_HASH)
        logger.info("  ✓ client_ip_hash column ensured.")

        await conn.execute(ADD_CORRELATION_UNIQUE)
        logger.info("  ✓ unique constraint on correlation_id ensured.")

        # ------------------------------------------------------------------
        # Step 2 — Create any tables that don't exist yet (dead_letter_queue)
        #          create_all with checkfirst=True never touches existing tables.
        # ------------------------------------------------------------------
        logger.info("Step 2/3: Running create_all (new tables only) …")
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn, checkfirst=True
            )
        )
        logger.info("  ✓ All tables in Base.metadata confirmed present.")

        # ------------------------------------------------------------------
        # Step 3 — Verify the schema looks correct
        # ------------------------------------------------------------------
        logger.info("Step 3/3: Verifying schema …")
        result = await conn.execute(text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'ingested_events'
            ORDER BY ordinal_position;
        """))
        columns = result.fetchall()
        logger.info("  ingested_events columns:")
        for col_name, col_type in columns:
            logger.info("    %-30s %s", col_name, col_type)

        result2 = await conn.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """))
        tables = [row[0] for row in result2.fetchall()]
        logger.info("  Public tables: %s", tables)

    await engine.dispose()
    logger.info("Migration complete. Engine disposed cleanly.")


if __name__ == "__main__":
    asyncio.run(run_migration())
