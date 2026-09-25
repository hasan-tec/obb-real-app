-- Migration 023: Shopify order cancellation marker on decisions
-- CANCELLATION_SYNC_PLAN.md §1.
--
-- order_cancelled_at is stamped on EVERY decision of an order once that order is cancelled in
-- Shopify, whatever the decision's status. It has to live on shipped and approved rows too: the
-- main failure (#OBB-14225, #OBB-15078) was a re-curate inheriting the order_id of a box that had
-- already shipped before the cancellation, so there was no pending row to mark.
--
-- Read via the existing decisions.order_id index (migration 017). Nullable and additive — safe to
-- run before the code deploy; old rows stay NULL until the backfill script stamps them.

ALTER TABLE decisions ADD COLUMN IF NOT EXISTS order_cancelled_at TIMESTAMPTZ;

COMMENT ON COLUMN decisions.order_cancelled_at IS
    'When the Shopify order this decision belongs to was cancelled. Set on every decision of the '
    'order regardless of status. Non-NULL blocks re-curate / override / create / replay from making '
    'a new box for that order.';
