# Shopify Cancellation Sync — Plan

> **Written:** 2026-09-26 · v2 (after independent audit) · branch `fix/shopify-cancellation-sync`
> **Billing:** not billed (Hasan's call). **Approved boxes:** flagged, never auto-closed.

## 0. What is actually wrong (verified against production, read-only)

- Shopify already sends cancellations to the registered **Order update** webhook, with
  `cancelled_at` / `cancel_reason` in the payload. `shopify_order_updated_webhook` ignores both.
  Nothing anywhere in the codebase reads `cancelled_at`.
- 33 orders show as cancelled in Shopify's own search; 31 still have a **pending** box in the engine.
  None have a VeraCore order yet.
- **26 of those 31 were created after the cancellation**, all by the 2026-09-17 bulk re-curate
  (`[Bulk-re-curated]` reason). Re-curate inherits the newest decision's `order_id` via
  `_prior_order_context` regardless of status, so it resurrects cancelled orders.
- **#OBB-15078 (dlcd_3@yahoo.com) shipped after it was cancelled** (cancelled 09-14 09:31, a box
  created 17:49 the same day and marked shipped, another pending 09-17). Staff follow-up needed.

The webhook gap is the smaller half. Blocking re-curate / override / create / replay from making a
box for a cancelled order is the bigger half.

## 1. Why a column, not a reason-text marker

The obvious marker is the rejected row's reason. It fails the main case: when an order is cancelled
after its box already **shipped**, there is no pending row to mark, so re-curate would still inherit
that order and create a new box (#OBB-14225, #OBB-15078 are exactly this). The marker has to live on
every decision of the order regardless of status.

**Migration 023** — `decisions.order_cancelled_at TIMESTAMPTZ` (nullable, additive). Looked up by the
already-indexed `decisions.order_id`, so the check is one indexed query — safe inside bulk re-curate.

## 2. Design

### 2.1 `_apply_shopify_cancellation(db, order_id, order_name, cancelled_at, cancel_reason, source, dry_run)`
Single helper used by the webhook, the daily pass and the backfill. Acts per decision by `order_id`
(never filtered by platform — 5 open decisions have a NULL platform).

| Decision status | Action |
|---|---|
| any | stamp `order_cancelled_at` where still NULL (separate update, so a missing column never blocks the close) |
| **pending** | `status → rejected`, reason `Auto-rejected: Shopify order #X cancelled YYYY-MM-DD (reason) — <source>.` **Conditional update** (`status = pending`); acts/logs/counts only on rows returned, so it can never overwrite a row staff approved a moment earlier |
| **approved** | never auto-closed. Activity warning, de-duplicated over 7 days (same pattern as the ship-to warning). Text tells staff to check VeraCore, and that stock and the draft shipment were already taken at approve |
| shipped / other | untouched, logged |

- `Auto-rejected:` prefix is deliberate: `get_rejected_kit_map` skips it, so a cancellation never
  blacklists that kit on the next re-curate.
- `customers.subscription_status` is **not** changed. A Shopify order cancel is order-level; the
  Recharge subscription can continue (4 of the cancelled orders were $0 renewals; duplicates get
  cancelled too). Deliberate difference from Cratejoy, whose cancel is subscription-level.
- No Google Sheet write (matches both existing reconcilers; the Sheet write is a synchronous
  full-sheet read and would risk Shopify's webhook timeout).
- No stock change: pending decisions have not taken stock (only approve / ship-from-pending do).

### 2.2 `_order_cancelled_at(db, order_id) -> str | None`
Indexed lookup on `decisions.order_id` with `order_cancelled_at` set. Fail-open (returns None and logs
an error) so a DB hiccup — or deploying before the migration runs — keeps today's behaviour rather
than blocking legitimate work.

### 2.3 Entry points

| Path | Change |
|---|---|
| `orders/updated` webhook | cancel branch runs **before** the customer lookup and returns straight after — skips quiz/address update, `recipient_ref` claim and ship-to sync (a late payload on a cancelled order carries a stale address) |
| `orders/create` webhook | no decision when the payload has `cancelled_at` or the order is already marked cancelled (covers out-of-order delivery) |
| Webhook replay | same check before creating a Shopify decision (replay routes `orders/updated` logs through the create path) |
| `_recurate_customer_core` | new status `blocked_cancelled` when the inherited order is cancelled. Covers single re-curate, bulk re-curate and the hold RESUME pass in one place |
| Single re-curate route | shows the `blocked_cancelled` message like the other block statuses |
| Bulk re-curate | checks **before** it rejects the old row; a pending row on a cancelled order is closed with the `Auto-rejected:` reason (not a staff-reject reason, which would blacklist the kit) and counted as skipped |
| Manual override | hard-blocked with a clear message when the inherited order is cancelled |
| Daily Shopify reconcile | new CANCEL pass before PAUSE: one `status:cancelled` search (same pattern as `get_on_hold_order_ids`), then `_apply_shopify_cancellation` per order. Safety net for missed webhooks |

### 2.4 `ShopifyClient.get_cancelled_orders()`
Paginated `orders(query:"status:cancelled", sortKey:UPDATED_AT, reverse:true)` →
`{numeric_id: {name, cancelled_at, cancel_reason}}`. Verified read-only against live Shopify
(33 orders, one page). The app has no `read_all_orders` scope, so Shopify only returns orders from the
last 60 days — fine: the webhook is real-time and the backfill covers history from `webhook_logs`.

### 2.5 `scripts/backfill_shopify_cancellations.py`
Sources: stored `webhook_logs` (chunked weekly — a single jsonb scan over ~33k rows times out) ∪
Shopify's cancelled search. Dry run by default prints every affected order and what would happen;
`--apply` calls `_apply_shopify_cancellation(source="backfill")`. Idempotent.

## 3. Deploy order

1. Hasan runs `migrations/023_order_cancelled_at.sql` in the Supabase SQL editor.
2. Verify the column exists (read-only check).
3. Merge + deploy.
4. Backfill dry run → Hasan reviews the list → `--apply`.

The code is fail-open on reads if the column is missing, but step 1 must happen first or the marker
is never written.

## 4. Tests — `tests/test_shopify_cancellation.py`
In-memory fake DB. pending → closed with `Auto-rejected:`; approved → warned not closed, warning
de-duplicated; shipped untouched; every decision stamped; conditional update doesn't overwrite a row
that turned approved; idempotent re-run; kit not blacklisted by `get_rejected_kit_map`; re-curate core
returns `blocked_cancelled`; non-cancelled order unaffected; reason format.

## 5. Out of scope
Refunds without a cancellation; partial line-item cancellations; Cratejoy (already reconciled daily);
un-pausing customers whose held order was then cancelled (3 today — flag to staff); making approve
paths conditional on status; Google Sheet updates.

## 6. For Sheena after deploy
- #OBB-15078 (dlcd_3@yahoo.com) shipped after cancellation — check with the customer.
- #OBB-14225 — shipped box plus a pending re-curated box on the same cancelled order.
- 3 customers whose on-hold order was then cancelled are stuck paused (302f1e65, a768e73e, 84f55649).
