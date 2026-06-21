-- Migration 018: Add quantity_available + inventory_synced_at to items
-- Populated daily by run_inventory_sync from VeraCore available_balance (item-level SKUs).
-- Shown in Curation Report so Sheena can see stock levels alongside risk.

ALTER TABLE items ADD COLUMN IF NOT EXISTS quantity_available INTEGER DEFAULT 0;
ALTER TABLE items ADD COLUMN IF NOT EXISTS inventory_synced_at TIMESTAMPTZ;
