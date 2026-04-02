-- ============================================================
-- Migration 002: Add 'replayed' to webhook_logs status CHECK
-- Run this in Supabase SQL Editor
-- ============================================================

-- Drop the old CHECK constraint and add new one with 'replayed'
ALTER TABLE webhook_logs DROP CONSTRAINT IF EXISTS webhook_logs_status_check;
ALTER TABLE webhook_logs ADD CONSTRAINT webhook_logs_status_check 
    CHECK (status IN ('received', 'processed', 'failed', 'duplicate', 'replayed'));
