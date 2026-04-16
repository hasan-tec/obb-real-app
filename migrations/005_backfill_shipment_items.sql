-- ============================================================
-- Migration 005: Backfill missing shipment_items
-- ============================================================
-- The historical import (import_history.py) inserts shipment_items
-- by copying kit_items for each kit assigned to a shipment.
-- For 657 shipments, the kit existed with kit_id linked, but
-- kit_items were added/updated AFTER the import ran, so those
-- shipments never got their shipment_items populated.
--
-- This migration backfills them by:
--   1. Finding all shipments that have a kit_id but zero shipment_items
--   2. For each, inserting all kit_items entries as shipment_items
--
-- This is idempotent (ON CONFLICT DO NOTHING).
-- Safe to re-run.
--
-- ⚠️ Run in Supabase SQL Editor
-- ============================================================

DO $$
DECLARE
  backfill_count INTEGER := 0;
  ship_count INTEGER := 0;
  item_count INTEGER := 0;
  r RECORD;
BEGIN
  -- Find shipments with kit_id but no shipment_items
  FOR r IN
    SELECT s.id AS shipment_id, s.kit_id, s.kit_sku
    FROM shipments s
    WHERE s.kit_id IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM shipment_items si WHERE si.shipment_id = s.id
      )
  LOOP
    ship_count := ship_count + 1;

    -- Copy all kit_items for this kit into shipment_items
    INSERT INTO shipment_items (shipment_id, item_id)
    SELECT r.shipment_id, ki.item_id
    FROM kit_items ki
    WHERE ki.kit_id = r.kit_id
    ON CONFLICT DO NOTHING;

    GET DIAGNOSTICS item_count = ROW_COUNT;
    backfill_count := backfill_count + item_count;
  END LOOP;

  RAISE NOTICE '[005] Backfilled % shipment_items across % shipments',
    backfill_count, ship_count;
END $$;

-- ============================================================
-- Verification query (run after):
-- ============================================================
-- SELECT COUNT(*) AS still_missing
-- FROM shipments s
-- WHERE s.kit_id IS NOT NULL
--   AND NOT EXISTS (
--     SELECT 1 FROM shipment_items si WHERE si.shipment_id = s.id
--   );
-- Expected: 0
-- ============================================================
