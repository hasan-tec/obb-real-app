-- Migration 009: Add expiry_date to items table
-- Items with an expiry_date in the past should auto-appear in DO NOT USE lists

ALTER TABLE items ADD COLUMN IF NOT EXISTS expiry_date DATE DEFAULT NULL;

COMMENT ON COLUMN items.expiry_date IS 'Optional expiry date. Expired items are automatically excluded from curation (appear in DO NOT USE).';
