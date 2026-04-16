-- ============================================================
-- OBB Curation Engine — Phase 2 Schema
-- Migration 007: Curation runs + customer results
-- Date: 2026-04-13
-- ============================================================

-- ───────── CURATION RUNS ─────────
-- One row per monthly curation report generation
CREATE TABLE IF NOT EXISTS curation_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    report_month TEXT NOT NULL,                -- e.g. '2026-04'
    ship_date DATE NOT NULL,                   -- target ship date for the month
    warehouse_minimum INTEGER NOT NULL DEFAULT 100,
    include_paused BOOLEAN DEFAULT FALSE,
    lookback_months INTEGER DEFAULT 4,
    status TEXT DEFAULT 'running' CHECK (status IN ('running', 'completed', 'error')),
    summary_json JSONB DEFAULT '{}'::jsonb,    -- executive overview data
    error_message TEXT,
    generated_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_curation_runs_month ON curation_runs (report_month);

-- ───────── CURATION RUN CUSTOMERS ─────────
-- Per-customer results within a curation run
CREATE TABLE IF NOT EXISTS curation_run_customers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id UUID NOT NULL REFERENCES curation_runs(id) ON DELETE CASCADE,
    customer_id UUID NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    projected_trimester INTEGER NOT NULL CHECK (projected_trimester BETWEEN 1 AND 4),
    needs_new_curation BOOLEAN DEFAULT FALSE,
    recommended_kit_id UUID REFERENCES kits(id),
    recommended_kit_sku TEXT,
    alternative_kit_skus JSONB DEFAULT '[]'::jsonb,   -- array of fallback SKUs in FIFO order
    reason TEXT,                                        -- why needs curation / why this kit
    blocking_item_count INTEGER DEFAULT 0              -- how many items this customer blocks
);

CREATE INDEX IF NOT EXISTS idx_curation_run_customers_run ON curation_run_customers (run_id);
CREATE INDEX IF NOT EXISTS idx_curation_run_customers_tri ON curation_run_customers (run_id, projected_trimester);

-- ───────── CURATION RUN ITEMS ─────────
-- Per-item risk results within a curation run, per trimester
CREATE TABLE IF NOT EXISTS curation_run_items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id UUID NOT NULL REFERENCES curation_runs(id) ON DELETE CASCADE,
    trimester INTEGER NOT NULL CHECK (trimester BETWEEN 1 AND 4),
    item_id UUID NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    blocked_count INTEGER DEFAULT 0,           -- how many customers received this item in window
    group_size INTEGER DEFAULT 0,              -- total customers in this trimester
    blocked_pct NUMERIC(5, 2) DEFAULT 0,       -- blocked_count / group_size * 100
    risk_level TEXT CHECK (risk_level IN ('HIGH', 'MEDIUM', 'LOW', 'NONE'))
);

CREATE INDEX IF NOT EXISTS idx_curation_run_items_run ON curation_run_items (run_id, trimester);

-- ───────── RLS (service role full access) ─────────
ALTER TABLE curation_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE curation_run_customers ENABLE ROW LEVEL SECURITY;
ALTER TABLE curation_run_items ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_role_all" ON curation_runs FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_all" ON curation_run_customers FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_all" ON curation_run_items FOR ALL USING (true) WITH CHECK (true);
