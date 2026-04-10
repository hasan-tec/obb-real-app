-- ============================================================
-- Migration 003: Foot & Hand Canvas — Split merged item into TWO
-- ============================================================
-- Problem: The import merged TWO sheet-tracked items into ONE:
--   Sheet had:  FOOT&HANDCANVAS+CANVASONLY
--               FOOT&HANDCANVAS+NONTOXICINKPADONLY
--   DB has:     FOOT&HANDCANVAS+COMPLETE  (af5febc5...)  ← had metadata
--               FOOT&HANDCANVAS+COMPLETESET (fcc1eb45...) ← dirty dup
--
-- Fix: Create two new canonical items, remap all kit_items +
--      shipment_items references to point at BOTH new items,
--      then delete the two old merged items.
--
-- Affected kits (17 total):
--   OBB-BT-21, OBB-BV-21, OBB-BV-31, OBB-BZ-21/22/23/24,
--   OBB-CC-41, OBB-CD-21, OBB-CG-41/42/43/44, OBB-CK-21  ← uses COMPLETE
--   OBB-BO-31, OBB-BO-41, OBB-BP-21                       ← uses COMPLETESET
--
-- ⚠️  Run this in Supabase SQL Editor.
-- ⚠️  After running: fill in unit_cost for both new items (cost was $0.88 combined).
-- ============================================================

BEGIN;

-- ── STEP 1: Insert the two new canonical items ──────────────────
INSERT INTO items (name, sku, category, has_sizing, unit_cost, is_therabox, notes)
VALUES
  (
    'Foot & Hand Canvas - Canvas Only',
    'OBB-FOOT&HANDCANVAS+CANVASONLY',
    'Baby Non-consumable',
    false,
    NULL,
    false,
    'Split from COMPLETE SET during data cleanup — update unit_cost'
  ),
  (
    'Foot & Hand Canvas - Non-Toxic Ink Pad Only',
    'OBB-FOOT&HANDCANVAS+NONTOXICINKPADONLY',
    'Baby Non-consumable',
    false,
    NULL,
    false,
    'Split from COMPLETE SET during data cleanup — update unit_cost'
  );

-- ── STEP 2: Remap all references via DO block ───────────────────
DO $$
DECLARE
  canvas_id       UUID;
  inkpad_id       UUID;
  old_complete    UUID := 'af5febc5-8d3a-4ddc-9560-18f7fdaaa768'; -- OBB-FOOT&HANDCANVAS+COMPLETE
  old_completeset UUID := 'fcc1eb45-2a03-44c6-9e0c-ac9c63e3b410'; -- OBB-FOOT&HANDCANVAS+COMPLETESET
  kit_rows_int    INTEGER;
  ship_rows_int   INTEGER;
BEGIN
  SELECT id INTO canvas_id FROM items WHERE sku = 'OBB-FOOT&HANDCANVAS+CANVASONLY';
  SELECT id INTO inkpad_id FROM items WHERE sku = 'OBB-FOOT&HANDCANVAS+NONTOXICINKPADONLY';

  RAISE NOTICE '[003] canvas_id=%, inkpad_id=%', canvas_id, inkpad_id;

  -- ── kit_items: COMPLETE → both new items ──────────────────────
  INSERT INTO kit_items (kit_id, item_id, quantity)
    SELECT kit_id, canvas_id, quantity FROM kit_items WHERE item_id = old_complete
    ON CONFLICT DO NOTHING;
  INSERT INTO kit_items (kit_id, item_id, quantity)
    SELECT kit_id, inkpad_id, quantity FROM kit_items WHERE item_id = old_complete
    ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS kit_rows_int = ROW_COUNT;
  DELETE FROM kit_items WHERE item_id = old_complete;
  RAISE NOTICE '[003] kit_items COMPLETE: remapped % kits', kit_rows_int;

  -- ── kit_items: COMPLETESET → both new items ───────────────────
  INSERT INTO kit_items (kit_id, item_id, quantity)
    SELECT kit_id, canvas_id, quantity FROM kit_items WHERE item_id = old_completeset
    ON CONFLICT DO NOTHING;
  INSERT INTO kit_items (kit_id, item_id, quantity)
    SELECT kit_id, inkpad_id, quantity FROM kit_items WHERE item_id = old_completeset
    ON CONFLICT DO NOTHING;
  DELETE FROM kit_items WHERE item_id = old_completeset;
  RAISE NOTICE '[003] kit_items COMPLETESET: remapped';

  -- ── shipment_items: COMPLETE → both new items ─────────────────
  -- Insert inkpad row for every shipment that had the old complete item
  INSERT INTO shipment_items (shipment_id, item_id)
    SELECT shipment_id, inkpad_id FROM shipment_items WHERE item_id = old_complete
    ON CONFLICT DO NOTHING;
  -- Insert canvas row
  INSERT INTO shipment_items (shipment_id, item_id)
    SELECT shipment_id, canvas_id FROM shipment_items WHERE item_id = old_complete
    ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS ship_rows_int = ROW_COUNT;
  -- Delete the old merged rows
  DELETE FROM shipment_items WHERE item_id = old_complete;
  RAISE NOTICE '[003] shipment_items COMPLETE: expanded % shipments', ship_rows_int;

  -- ── shipment_items: COMPLETESET → both new items ──────────────
  INSERT INTO shipment_items (shipment_id, item_id)
    SELECT shipment_id, inkpad_id FROM shipment_items WHERE item_id = old_completeset
    ON CONFLICT DO NOTHING;
  INSERT INTO shipment_items (shipment_id, item_id)
    SELECT shipment_id, canvas_id FROM shipment_items WHERE item_id = old_completeset
    ON CONFLICT DO NOTHING;
  DELETE FROM shipment_items WHERE item_id = old_completeset;
  RAISE NOTICE '[003] shipment_items COMPLETESET: expanded';

  -- ── Delete the two old merged items ───────────────────────────
  -- (item_alternatives rows auto-deleted via ON DELETE CASCADE)
  DELETE FROM items WHERE id = old_complete;
  DELETE FROM items WHERE id = old_completeset;

  RAISE NOTICE '[003] DONE. Two old merged items deleted. Verify: ';
  RAISE NOTICE '[003]   SELECT * FROM kit_items ki JOIN items i ON i.id=ki.item_id WHERE i.sku LIKE ''%%CANVAS%%'';';
END $$;

COMMIT;

-- ── Verification query (run after commit) ──────────────────────
-- SELECT k.sku as kit_sku, i.sku as item_sku, i.name
-- FROM kit_items ki
-- JOIN kits k ON k.id = ki.kit_id
-- JOIN items i ON i.id = ki.item_id
-- WHERE i.sku LIKE '%CANVAS%'
-- ORDER BY k.sku;
