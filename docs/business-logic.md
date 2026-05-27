# OBB Curation Engine — Business Logic

## 1. Trimester Calculation

When a webhook arrives, the engine calculates the customer's trimester based on their due date and the current ship date.

```
T4 cutoff  = ship_date + 19 days         (≤ this → T4 postpartum)
T3 cutoff  = T4 cutoff + 13 weeks        (≤ this → T3)
T2 cutoff  = T3 cutoff + 14 weeks        (≤ this → T2)
beyond T2  = T1
```

- **T1** — Early pregnancy (first/early second trimester)
- **T2** — Mid pregnancy
- **T3** — Late pregnancy
- **T4** — Postpartum (baby already born or due very soon)

The ship date for live webhooks is today's date. The ship date for the monthly curation report is the 14th of that month.

---

## 2. Kit FIFO (Age Rank) Selection

OBB ships kits in alphabetical batch order — oldest SKUs ship first to prevent expiry buildup. The `age_rank` determines priority:

```
Single-letter prefix (A–Z):    rank = letter position  (A=1, B=2 ... Z=26)
Double-letter prefix (AA–ZZ):  rank = (first × 26) + second  (AA=27, CK=89)
Welcome kit prefix (WK):       rank = 10001+  (always after regular kits)
```

Examples: `OBB-A41 KITS` → rank 1, `OBB-CK-41 KITS` → rank 89, `OBB-WK-C1 KITS` → rank 10003.

**Selection rule:** Find all kits where `trimester` matches, `quantity_available > 0`, size matches (or universal). Sort by `age_rank` ascending, take the first result.

---

## 3. Welcome Kit (WK) Logic

Welcome kits (`is_welcome_kit = true`) are only for a customer's **first shipment**. The engine checks the `shipments` table — if the customer has zero prior shipments, they're eligible for a WK.

WK kits are trimester-specific (`OBB-WK-C1 KITS` = T1 collab 1, `OBB-WK-D2 KITS` = T2 daddy kit, etc.). Same FIFO selection applies within the WK pool.

---

## 4. Size Matching

Clothing sizes come from the customer's quiz answer (`q_size`). The engine normalizes to `S / M / L / XL`:
- `"med"`, `"medium"` → `M`
- `"lrg"`, `"large"` → `L`
- `"xl"`, `"xlarge"`, `"xxl"`, `"2xl"` → `XL` (XXL groups with XL per OBB spec)

Kits with `size_variant = NULL` are **universal** — included in selection for any customer size, or when the customer has no size recorded.

---

## 5. Duplicate Blocking

`(customer_id, kit_sku, year-month)` must be unique in the `shipments` table. If the same combination already exists, the decision is flagged `needs_curation` with reason `duplicate_shipment` and not auto-assigned.

---

## 6. Decision Statuses

| Status | Meaning |
|--------|---------|
| `pending` | Waiting for admin review/approval |
| `approved` | Approved — VeraCore push succeeded or in queue |
| `rejected` | Manually rejected |
| `needs_curation` | Engine couldn't auto-assign — needs manual kit selection |
| `shipped` | Manually marked as shipped |

---

## 7. VeraCore Push (Fulfillment)

On approve:
1. Look up customer address from `decisions` / `customers`
2. Look up `veracore_sku` from `kits` table (falls back to `sku` if null)
3. Build SOAP `AddOrder` payload: customer info, ship-to address, Offer ID + qty 1
4. POST to `{VERACORE_BASE_URL}/VeraCore.Services.asmx`
5. Success → store `veracore_order_id`, set `veracore_status = 'submitted'`
6. Failure → set `veracore_status = 'failed'`, store error in `veracore_last_error`, log to `activity_log`

**Idempotency:** If `veracore_order_id` is already set, the push is skipped — safe to retry without duplicate orders.

**Freight service:** If `app_settings.veracore_freight_service` is set, a `<Shipping>` block is included. If blank, VeraCore uses the offer's default carrier (configured in VeraCore admin by Brian).

---

## 8. Manual Override

Admin can override the auto-assigned kit on any decision via the Override Kit button on the customer detail page. The override:
- Sets a new `kit_sku` on the decision
- Decrements `quantity_available` on the new kit
- Logs to `activity_log` with actor, old kit, new kit, reason

---

## 9. Monthly Curation Report

Runs automatically on the 1st of each month (06:00 UTC) or manually via UI. For each active customer:
- Recalculates trimester using ship date = 14th of the report month
- Finds the best kit (same FIFO + size logic)
- Checks for duplicates vs prior shipments
- Generates `kit_assignment`, `decision_type`, and `reason`

Results stored in `curation_runs` + `curation_report_details`. Admins bulk-approve from the UI.

---

## 10. Forward Planner

Projects inventory needed for the next N months based on active customers and their expected trimesters at each future ship date. Admins can "commit" specific quantities to reserve stock. Committed items tracked in `committed_items`.

---

## 11. Re-Curation

If a decision is rejected or wrong, admin triggers re-curation via the Recurate button. Re-runs the decision engine fresh for that customer and creates a new `pending` decision.

---

## 12. Order Type Detection

- `new` — first subscription box for this customer
- `renewal` — subsequent box
- `gift` — detected via `GIFT` in the subscription plan SKU from Shopify line items

---

## 13. Platform Sources

- **Shopify** — primary. Webhook at `POST /webhooks/shopify/orders/create`. Quiz answers in `note_attributes`.
- **Cratejoy** — secondary. Webhook at `POST /webhooks/cratejoy/order`. Quiz answers in order fields.

Both use HMAC-SHA256 for webhook verification. Invalid signatures are rejected with 401.
