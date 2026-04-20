-- Migration 011: Codify is_universal column (already exists in prod via Supabase UI)
-- This ensures fresh DB bootstraps from migrations won't crash the engine.

ALTER TABLE kits ADD COLUMN IF NOT EXISTS is_universal BOOLEAN DEFAULT FALSE;

COMMENT ON COLUMN kits.is_universal IS 'When TRUE, kit ships to ALL customers regardless of clothing_size. When FALSE, engine matches by size_variant digit.';
