-- Migration 008: Committed Items for Forward Planner
-- Phase 2, Step 6 — Tracks which items Ting has committed for each month's curated box
-- This enables "Mode B" in the Forward Planner — committed items propagate as blocked into future months

CREATE TABLE IF NOT EXISTS curation_committed_items (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    report_month TEXT NOT NULL,           -- e.g. '2026-05'
    trimester INTEGER NOT NULL CHECK (trimester BETWEEN 1 AND 4),
    item_id UUID NOT NULL REFERENCES items(id),
    committed_by TEXT DEFAULT 'ting',
    committed_at TIMESTAMPTZ DEFAULT NOW(),
    notes TEXT,
    UNIQUE(report_month, trimester, item_id)
);

CREATE INDEX IF NOT EXISTS idx_committed_month ON curation_committed_items(report_month);
CREATE INDEX IF NOT EXISTS idx_committed_trimester ON curation_committed_items(report_month, trimester);

ALTER TABLE curation_committed_items ENABLE ROW LEVEL SECURITY;
CREATE POLICY "service_role_all" ON curation_committed_items FOR ALL USING (true) WITH CHECK (true);
