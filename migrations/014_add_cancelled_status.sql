-- Migration 014: allow 'cancelled' as a valid decision status
ALTER TABLE decisions DROP CONSTRAINT IF EXISTS decisions_status_check;
ALTER TABLE decisions ADD CONSTRAINT decisions_status_check
  CHECK (status IN ('pending','approved','shipped','override','rejected','cancelled'));
