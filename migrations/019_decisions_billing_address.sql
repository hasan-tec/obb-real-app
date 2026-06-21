-- Migration 019: Snapshot purchaser (billing) and recipient (shipping) names on decisions
-- Enables VeraCore Ordered By = buyer, Ship To = gift recipient for split-address orders.
-- Columns are nullable — pre-existing decisions and Cratejoy orders keep NULL (self-purchase fallback).

ALTER TABLE decisions ADD COLUMN IF NOT EXISTS ship_first_name    TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS ship_last_name     TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS billing_first_name TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS billing_last_name  TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS billing_address1   TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS billing_city       TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS billing_state      TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS billing_zip        TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS billing_country    TEXT;
