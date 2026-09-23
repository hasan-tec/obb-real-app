-- Migration 022 (Threads 20/21): recipient profiles + per-decision ship-to + tracking idempotency
-- One change set, all columns planned up front (THREADS_20_21_FIX_PLAN.md A1).
-- Safe to run before the code deploy: while every recipient_ref is NULL the new unique index
-- enforces exactly what idx_customers_email_lower enforced (one row per email).

-- (1) Recipient profiles: one customers row per RECIPIENT. The same email may appear on several rows.
--     recipient_ref  = 'cj:<cratejoy subscription id>' | 'rc:<recharge subscription id>' | 'name:<normalized name>'
--                      NULL on legacy rows until claimed (scripts/backfill_recipient_refs.py, or the first matching order).
--     recipient_name = who the boxes on this profile ship to (used for matching; not the purchaser).
ALTER TABLE customers ADD COLUMN IF NOT EXISTS recipient_ref  TEXT;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS recipient_name TEXT;
DROP INDEX IF EXISTS idx_customers_email_lower;
CREATE UNIQUE INDEX IF NOT EXISTS idx_customers_email_recipient
    ON customers (LOWER(email), COALESCE(recipient_ref, ''));
CREATE INDEX IF NOT EXISTS idx_customers_email_lower_nonunique ON customers (LOWER(email));
CREATE INDEX IF NOT EXISTS idx_customers_recipient_ref
    ON customers (recipient_ref) WHERE recipient_ref IS NOT NULL;

-- (2) Per-decision ship-to snapshot. NULL on old rows → readers fall back to customers (decision_ship_to()).
--     ship_to_source: 'order' (from Shopify/Cratejoy) | 'manual' (reserved for the separately-quoted
--     manual Ship-To override). Sync code never overwrites 'manual'.
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS ship_address1  TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS ship_address2  TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS ship_city      TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS ship_state     TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS ship_zip       TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS ship_country   TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS ship_phone     TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS ship_to_source TEXT
    CHECK (ship_to_source IN ('order', 'manual'));

-- (3) Tracking idempotency for /decisions/upload-tracking: a tracking number already stored on a
--     decision is never pushed to Shopify/Cratejoy again (re-uploading a file is a no-op).
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS tracking_number    TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS tracking_pushed_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_decisions_tracking_number
    ON decisions (tracking_number) WHERE tracking_number IS NOT NULL;
