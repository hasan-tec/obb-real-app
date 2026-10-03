-- 024: when each box was marked shipped — powers the "Shipped on" date filter on the Decisions
-- page and in the Pirate Ship / Google Sheet exports (pick a date → only the boxes processed
-- that day). Set by single Ship, bulk Ship, the VeraCore shipment poll and the import scripts.
-- Existing shipped boxes are filled in by scripts/backfill_shipped_at.py (run after this).
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS shipped_at timestamptz;
CREATE INDEX IF NOT EXISTS idx_decisions_shipped_at ON decisions (shipped_at);
