-- =============================================================================
-- Phase 3 Schema Migration — pulse_analytics database
-- =============================================================================
-- Run this against: localhost:5444  db: pulse_analytics  user: pulse_admin
--
-- Every statement uses IF NOT EXISTS / DO $$ guards — safe to run multiple times.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Add missing columns to ingested_events
--    (table already exists from Phase 1 init-db.sql, but is missing Phase 3 cols)
-- -----------------------------------------------------------------------------

ALTER TABLE ingested_events
    ADD COLUMN IF NOT EXISTS ingested_at_unix DOUBLE PRECISION NOT NULL DEFAULT 0.0;

ALTER TABLE ingested_events
    ADD COLUMN IF NOT EXISTS client_ip_hash VARCHAR(64) NOT NULL DEFAULT '';

-- -----------------------------------------------------------------------------
-- 2. Add the unique constraint on correlation_id (idempotency key)
--    Wrapped in DO $$ so it doesn't error if it already exists.
-- -----------------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_ingested_events_correlation_id'
          AND conrelid = 'ingested_events'::regclass
    ) THEN
        ALTER TABLE ingested_events
            ADD CONSTRAINT uq_ingested_events_correlation_id UNIQUE (correlation_id);
    END IF;
END
$$;

-- -----------------------------------------------------------------------------
-- 3. Create dead_letter_queue table (missing entirely)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    failed_payload  JSONB           NOT NULL,
    error_reason    TEXT            NOT NULL,
    attempted_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    resolved        BOOLEAN         NOT NULL DEFAULT FALSE
);

-- -----------------------------------------------------------------------------
-- 4. Verify — run these SELECTs to confirm everything looks correct
-- -----------------------------------------------------------------------------

SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_name = 'ingested_events'
ORDER BY ordinal_position;

SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;
