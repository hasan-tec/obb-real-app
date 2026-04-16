-- Migration 010: Add projection_runs table for Forward Planner history
-- Stores completed forward projection results so history persists across server restarts

CREATE TABLE IF NOT EXISTS projection_runs (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    base_month  TEXT NOT NULL,
    params      JSONB DEFAULT '{}',
    result      JSONB DEFAULT '{}',
    status      TEXT DEFAULT 'done'
);

COMMENT ON TABLE projection_runs IS 'Forward Planner projection results — one row per completed run.';
COMMENT ON COLUMN projection_runs.params IS 'Input parameters: horizon_months, ship_day, warehouse_min, recency_months, include_paused.';
COMMENT ON COLUMN projection_runs.result IS 'Full projection output from project_forward().';
