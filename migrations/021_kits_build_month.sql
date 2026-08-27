-- 021_kits_build_month.sql
--
-- Records the month a kit was built, for kits that are NOT on the monthly cadence.
--
-- Monthly kits carry their build month in the SKU letters (CN=Jun, CO=Jul, CP=Aug, CQ=Sep),
-- which curation_report.month_to_age_rank() converts to and from age_rank. The four T1
-- renewal kits are built roughly once a year, so their age_rank (69/68/67/28) is only an
-- ordering and says nothing about when they shipped. Without a real month, blocked_kits()
-- had to guess by list position -- which under-blocked BQ-11 for T3 and blocked the wrong
-- kit entirely for T4.
--
-- Values are taken from the kit names and confirmed against Sheena's "OBB Kit Shipping
-- Schedule" sheet (2026 tab, T1 Renewals row): BP11 in the Feb and Mar windows,
-- "BP11 - BQ11 (50)" in the Apr 15 window, BQ11 from May 15 onward.
--
-- Safe to re-run.

ALTER TABLE kits ADD COLUMN IF NOT EXISTS build_month text;

COMMENT ON COLUMN kits.build_month IS
    'Month the kit was built, as YYYY-MM. Only needed for kits whose SKU letters do not '
    'encode a month (the T1 renewal kits). Read by curation_report.t1_build_month(); when '
    'NULL that function falls back to parsing the month out of kits.name.';

UPDATE kits SET build_month = '2026-04'
 WHERE sku = 'OBB-BQ-11 KITS' AND build_month IS DISTINCT FROM '2026-04';

UPDATE kits SET build_month = '2025-07'
 WHERE sku = 'OBB-BP-11 KITS' AND build_month IS DISTINCT FROM '2025-07';

UPDATE kits SET build_month = '2025-04'
 WHERE sku = 'OBB-BO-11 KITS' AND build_month IS DISTINCT FROM '2025-04';

UPDATE kits SET build_month = '2024-08'
 WHERE sku = 'OBB-AB-11 KITS' AND build_month IS DISTINCT FROM '2024-08';

-- Verify:
--   SELECT sku, name, build_month FROM kits WHERE trimester = 1 AND is_welcome_kit = false;
