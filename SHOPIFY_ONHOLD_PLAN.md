# Shopify "On Hold" → Engine Pause — Plan (Thread 17)

> **Problem owner:** Hasan · **Raised by:** Sheena (Thread 17) · **Scope:** Shopify + paused-gating (Cratejoy verdict in §10)
> **Status:** BUILT + LIVE — shipped 2026-07-20. First live run completed: 7 on-hold orders → 7 decisions auto-rejected `[Shopify hold]`, 6 unique customers paused (Elena Gamble had 2 on-hold orders). Scheduler wired for daily runs going forward.

---

## 1. Problem (Thread 17)

A customer asked to pause their subscription (miscarriage). Staff place the subscription **On Hold in Shopify**, which should stop fulfillment. But the engine still shows the decision as **For Fulfillment**, so an on-hold box can be processed/shipped unless a human catches it.

**Concrete live case (re-verified 2026-07-20):** order **#OBB-14011** (id `7327473238305`) is **ON_HOLD** in Shopify with hold note *"Miscarried"*. In our DB the customer is **Tammi Collins** <collinscrew5@gmail.com> (the buyer — "Amanda Collins" is the gift recipient), decision `60c8f63b` still **pending** with kit **OBB-AB-11**, sub status still **active**.

**It's not just her.** Live check found **7 orders currently ON_HOLD** store-wide, and **all 7 map to live pending decisions** in the engine (holds are also used for "no due date yet" chasing):

| Order | Hold note | Engine decision |
|---|---|---|
| #OBB-13268 | No due date, reached out twice | pending (Elena M Gamble) |
| #OBB-13672 | No due date | pending (William J Woodley) |
| #OBB-13752 | No response yet for due date | pending (Elena M Gamble) |
| #OBB-14011 | **Miscarried** | pending OBB-AB-11 (Tammi Collins) |
| #OBB-14055 | No due date | pending (Lynsey K Joy) |
| #OBB-14075 | No due date | pending (Sherece Franke) |
| #OBB-14078 | (on hold) | pending (Barbara Benson) |

## 2. Root cause (verified in code)

- Shopify feeds are **orders/create**, **customers/update**, **orders/updated** (quiz/address only). **None carry fulfillment-hold status** (REST order payloads don't expose ON_HOLD; the hold lives on the *fulfillment order*).
- `subscription_status='paused'` exists in schema but is **cosmetic** — nothing reads it to skip curation, block approve, or block ship. `assign_kit()` (app.py:1677) never looks at it. Approve/ship/bulk handlers never look at it. **0 customers are currently `paused`** — the only writer (CJ webhooks) is dead.
- Staff can already set "Paused" manually via customer Edit — it currently does nothing. After §5, it becomes a real safety switch.

## 3. Verified facts (2026-07-20 live checks — old §12 questions answered)

1. **Scopes ✓** — token exchange returns `write_assigned_fulfillment_orders, write_customers, write_merchant_managed_fulfillment_orders, write_orders`. Fulfillment-order READ is included (write implies read). No Dev Dashboard change needed.
2. **`decisions.order_id` format ✓** — Shopify decisions store the numeric order id (`str(payload["id"])`, app.py:2080), exactly what `_to_order_gid()` expects.
3. **`fulfillment_status:on_hold` is a documented `orders` search filter ✓** (accepted values: unshipped, shipped, fulfilled, partial, scheduled, **on_hold**, unfulfilled, request_declined) — and verified working live against the OhBaby tenant (returned the 7 orders above, ~7 GraphQL cost points per page of 50).
4. **Scale ✓** — **1,370 open Shopify decisions** (pending/approved with order_id). Per-decision FO polling would be 1,370 calls/day; the on-hold *search* approach is **1 call per 50 on-hold orders** (currently 1 page). This changes the job design — see §4.
5. **`decisions.status` CHECK constraint** allows only `('pending','approved','shipped','override','rejected')` (001_initial_schema.sql:121). A new `on-hold` status **requires a migration** → v1 reuses `rejected` (§6).
6. **`customers.subscription_status` CHECK** already allows `paused` ✓ — no migration for v1.

## 4. Solution — daily Shopify hold reconciliation (PULL, set-based)

Mirrors `cratejoy_daily_reconcile` (app.py:930) but **inverted for efficiency**: instead of asking Shopify about each of 1,370 open decisions, ask Shopify once for the (tiny) set of on-hold orders and intersect.

### 4.1 New `shopify_client.py` methods
```python
def get_on_hold_order_ids(self) -> set[str]:
    # paginated orders(query:"fulfillment_status:on_hold") -> {"7327473238305", ...}

def get_fulfillment_order_statuses(self, order_numeric_id) -> list[str]:
    # raw FO status list e.g. ["ON_HOLD"] / ["OPEN"] / ["CLOSED"] — used by the
    # RESUME check + dry-run detail. NOT get_open_fulfillment_orders (that can't
    # tell on-hold from already-shipped: both return []).
```

### 4.2 The job: `shopify_daily_reconcile(db, dry_run=False)`
**PAUSE pass**
1. `held = get_on_hold_order_ids()` (1–2 API calls).
2. Load open Shopify decisions (`status in ('pending','approved')`, `platform='shopify'`, `order_id not null`, paginate past 1000).
3. For each decision whose `order_id ∈ held`:
   - **pending** → `status='rejected'`, `reason="[Shopify hold] order on hold in Shopify — auto-paused by daily reconcile"`, set customer `subscription_status='paused'`. The `[Shopify hold]` prefix is the **marker** that distinguishes auto-pause from a staff pause (needed for safe auto-resume).
   - **approved** → **do NOT silently reject** (may already be pushed to VeraCore; an engine reject can't recall a VC order). Log loudly + `log_activity` warning "approved decision X is for an ON_HOLD Shopify order — check VeraCore/warehouse manually". Still set the customer `paused`.

**RESUME pass**
4. Load customers `subscription_status='paused'`, `platform in ('shopify','both')` that have a `[Shopify hold]`-rejected decision (marker check ⇒ staff-paused customers are never auto-resumed).
5. For each, take the held `order_id` from that rejected decision; if `order_id ∉ held` **and** `get_fulfillment_order_statuses(order_id)` contains an open state (OPEN/IN_PROGRESS/SCHEDULED) → set `subscription_status='active'` and re-curate via the shared helper (§4.4). If the order is now CLOSED/CANCELLED instead, just log — nothing to resume.
6. Idempotent (skip already-matching rows), every change logged + `log_activity`'d, returns `{held_orders, scanned, paused, approved_flagged, resumed, skipped}`.

### 4.3 Wiring
- Scheduler: `last_shopify_reconcile_day` guard + block after the CJ reconcile (app.py ~1077), gated on `shopify_fulfillment_enabled()`, once/day after 7 AM UTC, `_schedule_lock(f"shopify_reconcile_{day}")`.
- Admin route: `POST /api/shopify/hold-reconcile?dry_run=true` — mirrors `/api/cratejoy/daily-reconcile`.

### 4.4 Re-curate helper (small refactor)
Extract the core of `POST /customers/{id}/recurate` (app.py:7236 — assign_kit + prior-order-context + insert) into `_recurate_customer_core(db, customer_id, reason_prefix)` used by both the route and the resume pass. Route behavior unchanged.

## 5. Make `paused` actually gate the engine (both platforms)

Callers gate (assign_kit stays pure). Exact spots:
1. **orders/create webhook** (~app.py:2069): if customer `paused` → skip decision creation (still upsert customer data), log + activity.
2. **CJ daily sync** `process_cratejoy_box` (~app.py:666): if `cust_row.subscription_status == 'paused'` → return `"skip_paused"`.
3. **Re-curate route** (app.py:7236): block with clear msg if `paused` (helper takes an `allow_paused=False` param so the resume pass can pass through after un-pausing).
4. **Approve** — single (app.py:6760) + bulk (app.py:7640): block/skip if customer `paused` (add `subscription_status` to the joined customer select), log msg "customer is paused/on hold".
5. **Ship** — single (app.py:7148) + bulk: same block.

## 6. Design decision: reject vs a new `on-hold` status

- **v1 (build now):** reuse `status='rejected'` + `[Shopify hold]` reason marker. No migration (the status CHECK constraint would need one — §3.5). Reversible via re-curate on release.
- **v2 (defer):** proper `on-hold` decision status via migration if Sheena wants the visual distinction in filters.

## 7. UI/UX (explicitly requested)

- **customer_detail.html** — add a **⏸️ Paused / On Hold banner** (amber, same pattern as the existing cancelled-expired/cancelled-prepaid banners at lines 57–74): "Subscription Paused / On Hold — the engine will not curate, approve, or ship for this customer. Resumes automatically when the Shopify hold is released (daily), or set Status back to Active via Edit."
- **customers.html** — already renders a yellow **Paused** badge (lines 321/380) ✓ nothing to do.
- Decisions page — auto-paused rows appear as `rejected` with the `[Shopify hold]` reason ✓ nothing to do in v1.

## 8. Safety

- **Dry-run first** via the admin route; review the pause/resume list (expected first run: the 7 orders in §1 → 7 pending decisions rejected + 7 customers paused).
- **Backup** `scripts/backup_obb_tables.py` before first live run.
- **Read-only against Shopify** — the reconcile never writes to Shopify.
- Idempotent + logged; no-op when Admin API creds absent (`shopify_fulfillment_enabled()`).
- Auto-resume only touches customers with the `[Shopify hold]` marker — staff-paused customers stay paused.

## 9. Interim action

Not needed as a separate step — the first live reconcile run handles #OBB-14011 (and the other 6) automatically.

## 10. Cratejoy — investigation verdict (2026-07-20)

**No Cratejoy-specific hold job needed.** Evidence:
- Cratejoy's API has **no "paused" subscription status**; **"suspended" is deprecated** (docs.cratejoy.com/reference/subscription). Customer-side pause = **skip next renewal** (`skipped_date`) → no renewal → **no unshipped shipment is ever generated** → our pull-based `_cratejoy_daily_sync` (which only creates decisions from the unshipped queue) never creates a decision for it. Self-handling by design.
- A cancelled shipment behind an existing pending decision is already auto-rejected daily by `_cj_reconcile_pending_decisions` (app.py:798) ✓.
- CJ webhooks (the only historical writer of `paused`) are dead since 6/24 and being replaced by the daily poll anyway.
- The §5 gating covers the remaining case on both platforms: a staff member manually setting Paused now actually blocks curation/approve/ship.
- **Watch item (not built now):** if Cratejoy *postpones* (moves ship date) rather than cancels a shipment behind a pending decision, the decision keeps its old ship_date. If this shows up in practice, extend CJ reconcile Job 1 to compare the live shipment ship_date to the decision month.

## 11. Build order

1. `get_on_hold_order_ids()` + `get_fulfillment_order_statuses()` in `shopify_client.py`.
2. `_recurate_customer_core()` refactor (route keeps behavior).
3. `shopify_daily_reconcile()` + `POST /api/shopify/hold-reconcile` admin route.
4. `paused` gating — webhook, CJ sync, recurate, approve ×2, ship ×2 (§5).
5. ⏸️ banner in `customer_detail.html` (§7).
6. Scheduler wiring (§4.3).
7. Dry-run → review 7 expected pauses → backup → first live run.
8. (Optional, deferred) Option B real-time webhooks `fulfillment_orders/placed_on_hold` / `hold_released` — payload shape still unverified (§13); the daily reconcile is the safety net regardless.

## 12. Acceptance

- Order placed On Hold in Shopify → its pending decision is auto-rejected with the `[Shopify hold]` marker within one daily cycle and drops out of fulfillment/export lists; customer shows ⏸️ Paused.
- Hold released → customer auto-resumes to active + re-curated. Staff-paused customers are never auto-resumed.
- A `paused` customer cannot be curated, approved, or shipped (single or bulk), on either platform.
- Approved-on-hold decisions are flagged (not silently rejected) for manual VeraCore follow-up.
- No behavior change for Cratejoy flows (§10).

## 13. Remaining open questions (deferred with Option B)

- Exact `order_id` field/payload shape of `fulfillment_orders/placed_on_hold` webhook — verify against a real captured event before building Option B.
