# OBB Curation Engine — Business Logic Map

How the engine decides which box goes to whom, and what each staff action does. Current as of 2026-10-06. Function names point into `app.py` unless noted.

## 1. How a box enters the system

| Source | Trigger | What happens |
|---|---|---|
| Shopify new order | `orders/create` webhook → `shopify_order_webhook` | Checks HMAC and duplicate event ids. Logs to `webhook_logs`. Finds or creates the recipient profile. Reads quiz data (due date, size). Runs `assign_kit`. Inserts one `pending` decision. |
| Shopify order change | `orders/updated` webhook | Syncs the ship-to address onto open decisions. Applies cancellations: open boxes are rejected with an `Auto-rejected:` reason, and `order_cancelled_at` is stamped (migration 023). |
| Shopify customer change | `customers/update` webhook | Updates customer details. |
| Cratejoy box due | Daily sync at 07:00 UTC → `_cratejoy_daily_sync` | Pulls Cratejoy's unshipped-shipment queue and creates one decision per box whose ship date has arrived (from 2026-06-01). Re-running it is safe. |
| Cratejoy changes | Daily reconcile → `cratejoy_daily_reconcile` | Rejects open boxes whose Cratejoy shipment was cancelled. Refreshes ship-to and ordered-by. Syncs subscription status. |
| Shopify on hold | Daily → `shopify_daily_reconcile` | A pending box whose order is ON_HOLD is rejected with a `[Shopify hold]` reason, and the customer is paused. When the hold is lifted, the customer is resumed and re-curated. A staff-set pause is never touched. |
| Manual | Customer page → Recurate, or **Add customer** | Same engine, run on demand. |
| Lost webhook | Webhooks page → Replay | Re-runs a logged webhook. **Replay guard** (`_replay_existing_box`): Shopify replays only order events, and never if the order already has a decision. Cratejoy replays never if the shipment already has an open box, or the customer already has a box this month. |

Order type (`_compute_order_type`): **renewal** if the profile has at least one real past shipment (ship date set and before today). Otherwise it is **new**.

## 2. Recipient profiles (one email, several babies)

A `customers` row is one **recipient**, not one email. A gift buyer who sends to two friends gets two profiles. `resolve_recipient_profile` matches an incoming order in this order:
1. the platform recipient ref,
2. a unique name match,
3. an unclaimed legacy profile,
4. otherwise a new profile is created. An Activity alert "New recipient profile" is written.

Each profile has its own due date, history and kit rotation.

## 3. Trimester

`calculate_trimester(due_date, ship_date)`:
```
due ≤ ship + 19 days                 → T4 (postpartum)
due ≤ that + 13 weeks                → T3
due ≤ that + 14 weeks                → T2
later                                → T1
```
Live decisions use today as the ship date. The monthly report uses the 14th of the month. The trimester is always recalculated from the due date at decision time. A daily job also refreshes `customers.trimester` so lists and filters stay correct.

## 4. Kit selection — `assign_kit`

Steps, in order:
1. **No due date** → `incomplete-data` (staff add the due date, then Recurate).
2. **Welcome or regular.** A profile with no shipments gets **welcome kits** (`is_welcome_kit`). Anyone else gets regular kits, in the trimester just calculated.
3. **Free stock.** `kits_with_free_stock`: free = `quantity_available` minus the **pending** boxes already holding that kit. A kit with 3 on hand and 3 pending boxes is full. Approved boxes are not subtracted again, because approval already took them off stock. If nothing has free stock → `needs-curation`, with a reason telling staff to add stock or kits.
4. **Size.** S/M/L/XL map to `size_variant` 1/2/3/4. Kits with `is_universal` fit everyone. A profile with no size gets universal kits only.
5. **No repeats.** A kit is dropped if:
   - the customer already received this kit SKU,
   - it shares any item with the items they received (including alternates from `item_alternatives`), or
   - staff rejected it **this cycle** (since their last shipment). Rejections that start with `Auto-rejected:` or `[Shopify hold]` are system rejections and don't count.
6. **FIFO.** The lowest `age_rank` wins: oldest batch letter first; welcome kits rank after regular ones (`compute_age_rank_from_sku`).

Result `decision_type`:

| Type | Meaning | What staff do |
|---|---|---|
| `auto` | A kit was found | Review, then approve or ship |
| `needs-curation` | No kit fits (stock, size or repeats) | Add or adjust kits, or Override |
| `incomplete-data` / `needs-data-entry` | Missing due date or size | Edit the customer, then Recurate |
| `skipped` | Cratejoy subscription cancelled or expired with no prepaid boxes left | Nothing |
| `manual-override` | Staff picked the kit | — |

## 5. Decision lifecycle and staff actions

```
pending ──approve──► approved ──ship──► shipped
   │                                      ▲
   └──────────────ship (direct)───────────┘
   └──reject──► rejected       (recurate → a fresh pending box)
```

| Action | Effect |
|---|---|
| **Approve** | Status `approved`. Kit stock goes down by 1. Pushes to VeraCore if it is configured (it runs in the background and is skipped if a VeraCore order id already exists, so retries never duplicate). |
| **Ship** | Status `shipped`, `shipped_at` = now. Stock goes down by 1 if the box was still pending. Writes the shipment-history row the engine uses for repeat blocking. |
| **Reject** | Status `rejected`. The kit is avoided on the next Recurate this cycle. |
| **Recurate** | Customer page: runs `assign_kit` again. It is blocked while the customer still has a pending box (reject or approve that first), and when the customer is paused or the order is cancelled. On the Decisions page (bulk): rejects the selected pending box and creates a fresh one in a single step. |
| **Override kit** | Staff choose from the kits the customer is eligible for (`get_eligible_override_kits`, same rules as the engine). |
| **Bulk** (approve, ship, reject, recurate) | Same rules, on the selection, in a background job with a progress banner. Boxes already being processed by another bulk job are skipped and reported. |
| **Paused / on-hold customer** | Approve and Ship are blocked. |

## 6. Shipping-day workflow (what ops does)

1. On the Decisions page, filter to the boxes you want. Then bulk **Ship**: the page lands on today's shipped boxes. Or use **Shipped on** = a date.
2. **Pirate Ship CSV** (`/decisions/export-csv`). With a **Shipped on** date set, it exports just that day's boxes. Buy the labels in Pirate Ship.
3. In Pirate Ship, use **Export tracking data**, then **Upload tracking** on the Decisions page (`/decisions/upload-tracking`). Each row is matched to exactly one decision (OBB ref, then order id, then email plus recipient name), and the tracking is pushed to that box's Shopify order or Cratejoy shipment. Re-uploading the same file changes nothing. Rows it can't match safely come back as `ambiguous` or `needs_review` and nothing is touched.
4. Optional: **Push to Sheet** sends the same filtered set to the Google Sheet.

## 7. Monthly curation report and forward planner

- **Curation report** (`curation_report.py`): runs automatically on the 3rd at 06:00 UTC, or on demand. It recalculates every active customer for that month's ship date (the 14th) and lists the kit each would get, shortfalls, and stock by kit. It reads only; it changes no decisions.
- **Forward planner** (`projection_engine.py`): projects kit and item demand N months ahead from due dates. "Commit items" reserves planned stock (`curation_committed_items`).

## 8. VeraCore (3PL)

- **Inventory and expiry sync**: daily at 04:00 UTC, or with the buttons on the VeraCore page. It updates kit and item stock from VeraCore offers.
- **Order push** on approve: SOAP `AddOrder`, built from the kit's `veracore_sku`. Freight service comes from `app_settings`. Failures are stored on the decision and can be retried from the UI.
- **Shipment poll**: built, but switched off in the scheduler (not in the SOW).
- With no VeraCore credentials, every VeraCore feature is skipped silently.

## 9. Rules worth knowing

- `shipments` is the source of truth for "what did this customer get". Imports and manual history entries go there.
- Rejections that start with `Auto-rejected:` (cancellations, stale-box cleanup) never block a kit.
- Every staff action writes to `activity_log`. Every webhook is kept in `webhook_logs`, with its raw payload.
