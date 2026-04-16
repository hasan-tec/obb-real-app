-- Migration 006: Fix trimester mismatches + has_sizing flag
-- Date: 2026-04-13
-- Context: 3 kits had DB trimester not matching their SKU encoding (second-to-last digit = trimester)
--          1 item (Willowcollective candle) had has_sizing=True incorrectly
-- Author: Hasan / OBB Agent

-- C1: Fix 3 kit trimester mismatches
-- OBB-BZ-32: SKU digit says T3, DB had T2
UPDATE kits SET trimester = 3, updated_at = now()
WHERE sku = 'OBB-BZ-32 KITS';

-- OBB-CB-23: SKU digit says T2, DB had T3
UPDATE kits SET trimester = 2, updated_at = now()
WHERE sku = 'OBB-CB-23 KITS';

-- OBB-CG-32: SKU digit says T3, DB had T2
UPDATE kits SET trimester = 3, updated_at = now()
WHERE sku = 'OBB-CG-32 KITS';

-- M5: Fix Willowcollective candle has_sizing flag (candle does not have clothing sizes)
-- Note: items table has no updated_at column, so we only update has_sizing
UPDATE items SET has_sizing = FALSE
WHERE name ILIKE '%willowcollective%midnightembercandle%'
   OR sku ILIKE '%willowcollective%midnightembercandle%';

-- Verify
SELECT sku, trimester FROM kits WHERE sku IN ('OBB-BZ-32 KITS', 'OBB-CB-23 KITS', 'OBB-CG-32 KITS');
SELECT sku, name, has_sizing FROM items WHERE name ILIKE '%willowcollective%midnightembercandle%';
