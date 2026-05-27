-- App-level key-value settings editable from the OBB Engine UI.
-- Initial keys:
--   veracore_freight_service  TEXT  — e.g. "OSM USPS Priority Mail"
--     Leave empty → no <Shipping> block sent in SOAP (VeraCore uses offer default).

CREATE TABLE IF NOT EXISTS app_settings (
  key        TEXT PRIMARY KEY,
  value      TEXT,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
