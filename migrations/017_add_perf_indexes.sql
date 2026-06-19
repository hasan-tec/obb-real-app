-- Migration 017: Add performance indexes for decisions and shipments tables
-- Speeds up the Decisions page (status/customer_id/order_id filters) and
-- the customers page shipment lookups (customer_id, ship_date).

CREATE INDEX IF NOT EXISTS idx_decisions_status
    ON decisions (status);

CREATE INDEX IF NOT EXISTS idx_decisions_customer_id
    ON decisions (customer_id);

CREATE INDEX IF NOT EXISTS idx_decisions_order_id
    ON decisions (order_id);

CREATE INDEX IF NOT EXISTS idx_shipments_customer_id
    ON shipments (customer_id);

CREATE INDEX IF NOT EXISTS idx_shipments_ship_date
    ON shipments (ship_date);
