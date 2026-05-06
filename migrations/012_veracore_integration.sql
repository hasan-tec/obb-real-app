-- 012_veracore_integration.sql
-- Phase 3 — VeraCore fulfillment integration
--
-- Adds:
--   1. VeraCore order tracking columns on `decisions`
--   2. `veracore_sync_log` table (audit trail — every API call, success or fail)
--   3. `kit_stock_alerts` table (low-stock warnings from daily inventory sync)
--   4. `veracore_sku` on `kits` (maps internal SKU → VeraCore OfferID when they differ)
--
-- All statements are idempotent (IF NOT EXISTS) — safe to re-run.

-- ─── 1. Decisions: VeraCore tracking ───
ALTER TABLE decisions
    ADD COLUMN IF NOT EXISTS veracore_order_id     TEXT,
    ADD COLUMN IF NOT EXISTS veracore_submitted_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS veracore_status       TEXT,   -- 'pending' | 'submitted' | 'failed' | 'shipped'
    ADD COLUMN IF NOT EXISTS veracore_last_error   TEXT,
    ADD COLUMN IF NOT EXISTS veracore_tracking     TEXT;

CREATE INDEX IF NOT EXISTS idx_decisions_veracore_status
    ON decisions(veracore_status)
    WHERE veracore_status IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_decisions_veracore_order_id
    ON decisions(veracore_order_id)
    WHERE veracore_order_id IS NOT NULL;

-- ─── 2. Sync log (audit trail for every VeraCore API interaction) ───
CREATE TABLE IF NOT EXISTS veracore_sync_log (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_at      TIMESTAMPTZ DEFAULT NOW(),
    sync_type   TEXT NOT NULL,   -- 'inventory' | 'order_submit' | 'shipment_poll'
    decision_id UUID REFERENCES decisions(id) ON DELETE SET NULL,
    request     JSONB,
    response    JSONB,
    status      TEXT NOT NULL,   -- 'ok' | 'fail'
    error       TEXT
);

CREATE INDEX IF NOT EXISTS idx_sync_log_run_at      ON veracore_sync_log(run_at DESC);
CREATE INDEX IF NOT EXISTS idx_sync_log_sync_type   ON veracore_sync_log(sync_type, run_at DESC);
CREATE INDEX IF NOT EXISTS idx_sync_log_decision_id ON veracore_sync_log(decision_id) WHERE decision_id IS NOT NULL;

-- ─── 3. Low-stock alerts (raised by daily inventory sync) ───
CREATE TABLE IF NOT EXISTS kit_stock_alerts (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kit_id     UUID REFERENCES kits(id) ON DELETE CASCADE,
    seen_qty   INTEGER NOT NULL,
    threshold  INTEGER NOT NULL DEFAULT 15,
    alerted_at TIMESTAMPTZ DEFAULT NOW(),
    resolved   BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMPTZ,
    note       TEXT
);

CREATE INDEX IF NOT EXISTS idx_stock_alerts_unresolved
    ON kit_stock_alerts(kit_id, alerted_at DESC)
    WHERE resolved = FALSE;

-- ─── 4. Kits: VeraCore SKU mapping ───
ALTER TABLE kits
    ADD COLUMN IF NOT EXISTS veracore_sku TEXT;

CREATE INDEX IF NOT EXISTS idx_kits_veracore_sku
    ON kits(veracore_sku)
    WHERE veracore_sku IS NOT NULL;
