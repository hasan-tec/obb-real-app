-- Migration 016: add order_type to decisions + backfill from shipment history
--
-- order_type: 'new' = customer had 0 prior shipments when decision was created
--             'renewal' = customer had ≥1 prior shipments
--
-- New decisions get this set by the Shopify/Cratejoy webhook handlers already.
-- Existing decisions are backfilled below using actual shipment history.

ALTER TABLE decisions ADD COLUMN IF NOT EXISTS order_type TEXT
    CHECK (order_type IN ('new', 'renewal'));

-- Backfill: for each decision, count shipments that shipped before the decision date.
-- 0 prior shipments → 'new', otherwise → 'renewal'.
UPDATE decisions d
SET order_type = CASE
    WHEN (
        SELECT COUNT(*)
        FROM shipments s
        WHERE s.customer_id = d.customer_id
          AND s.ship_date IS NOT NULL
          AND s.ship_date < d.created_at::date
    ) = 0 THEN 'new'
    ELSE 'renewal'
END
WHERE d.order_type IS NULL;
