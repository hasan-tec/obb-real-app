-- ============================================================
-- Migration 003: Add 'skipped' and 'needs-data-entry' to decisions_decision_type_check
-- Run this in Supabase SQL Editor
-- ============================================================

-- Drop the old constraint (only allows 'auto', 'manual-override', 'needs-curation', 'incomplete-data')
ALTER TABLE decisions DROP CONSTRAINT IF EXISTS decisions_decision_type_check;

-- Add updated constraint with 'skipped' (cancelled/expired subscription — no kit needed)
-- and 'needs-data-entry' (Cratejoy customer with no quiz data — manual entry required)
ALTER TABLE decisions ADD CONSTRAINT decisions_decision_type_check
    CHECK (decision_type IN (
        'auto',
        'manual-override',
        'needs-curation',
        'incomplete-data',
        'skipped',
        'needs-data-entry'
    ));
