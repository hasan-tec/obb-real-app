-- Migration 015: add needs_review flag for auto-created kits from VeraCore offer sync
ALTER TABLE kits ADD COLUMN IF NOT EXISTS needs_review BOOLEAN DEFAULT FALSE;
