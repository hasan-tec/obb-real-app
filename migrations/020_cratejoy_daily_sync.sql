-- Migration 020: Cratejoy daily-sync support (one change set, both columns planned up front)
--
-- (1) decisions.cratejoy_shipment_id — idempotency key for the daily shipment-poll cron.
--     One decision per Cratejoy shipment, retry-safe across daily re-runs and any late
--     webhook. Partial unique index so NULLs (Shopify/legacy rows) are unaffected.
--
-- (2) customers.history_pending — marks backlog Cratejoy customers imported WITHOUT their
--     real kit history yet. The monthly sweep (and the future daily cron) MUST skip
--     decision-creation for these until the flag is cleared, so a customer who is really
--     on box 3 is never sent a duplicate welcome kit. Defaults false; existing customers
--     are unaffected.

ALTER TABLE decisions ADD COLUMN IF NOT EXISTS cratejoy_shipment_id TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_decisions_cratejoy_shipment_id
    ON decisions (cratejoy_shipment_id)
    WHERE cratejoy_shipment_id IS NOT NULL;

ALTER TABLE customers ADD COLUMN IF NOT EXISTS history_pending BOOLEAN NOT NULL DEFAULT false;
