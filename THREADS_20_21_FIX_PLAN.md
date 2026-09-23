# Threads 20 + 21 — Tracking Upload, Per-Box Ship-To & Recipient Profiles (Fix Plan v3)

> **Status:** PLAN ONLY — nothing below is implemented yet.
> **Written:** 2026-09-23 · v3 (recipient profiles now IN scope, delivered free; manual Ship-To override form is OUT — quoted separately).
> **Deadlines:** Part A by **2026-09-30** (Oct 1 daily sync + first month-2 VeraCore pushes). Parts B–C right after.
> **Read `CLAUDE.md` first** — logging with context, `get_supabase()`, one migration per change set, surgical changes.
> Line numbers are from commit `bbf52ef` and WILL drift — **search by function name**.

---

## 🚦 Implementation status (branch `fix/threads-20-21`, 2026-09-23)

**Done (senior) — tested, not deployed:**
| Task | Where | Tests |
|---|---|---|
| A1 migration | `migrations/022_recipient_profiles_ship_to_tracking.sql` | — (run in Supabase) |
| A2 Cratejoy exact-shipment client | `cratejoy_client.py` `get_shipment`, `list_customer_shipments` (email "most recent" methods deleted) | via upload tests |
| A3/A4/B1/C1 pure helpers | `app.py` block "THREADS 20/21 HELPERS" (right after `_cj_shipment_ship_date`) | `tests/test_threads_20_21.py` |
| A5 tracking upload rewrite | `app.py` `upload_tracking` | `tests/test_upload_tracking.py` (incl. the 5× re-upload incident) |
| A6 `OBB Ref` column | 3 Pirate Ship exports | — |
| A7 VeraCore OrderID per box | `veracore_order_id_for()`; `submit_to_veracore`, `export_decisions_veracore_csv` | `test_veracore_order_id_unique_per_cratejoy_box` |
| B2 ship-to snapshot on create | Shopify webhook + replay, `process_cratejoy_box`; Cratejoy `unit` captured | `tests/test_recipient_profiles.py` |
| B3 readers use the box's ship-to | `_build_veracore_ship_to` (replaces `_build_ship_to_from_customer`), all 5 exports | — |
| B4 order edit → box ship-to | `_sync_order_ship_to_from_shopify` called from `shopify_order_updated_webhook` | `test_order_edit_updates_pending_box_and_flags_shipped_box` |
| B5 buyer address guard | `shopify_customer_webhook` | — |
| B6 Cratejoy reconcile full ship-to | `_cj_reconcile_pending_decisions` | — |
| B7 staff address edit → open boxes | `edit_customer` | — |
| C1/C2 recipient profiles | `load_email_profiles`, `ensure_profile_identity`, `resolve_recipient_profile`, `upsert_shopify_recipient_profile`, `pick_one_profile`; wired into Shopify webhook, replay, orders/updated, `process_cratejoy_box`, Cratejoy webhook/replay | `tests/test_recipient_profiles.py` |
| C3 status reconcile per profile | `_cj_reconcile_customer_statuses` | — |
| C4 page data (backend only) | `decisions_page` → `d._ship_to`, `d._recipient_count`; `customers_page` → `c._recipient_count`, `c._recipient_index`; `customer_detail` → `sibling_profiles`, `latest_ship_to` | — |
| D1 backfill script | `scripts/backfill_recipient_refs.py` (dry run tolerates migration not yet applied) | dry-run on prod |
| D2 split script | `scripts/split_recipient_profiles.py` | dry-runs: `../split_dry_runs_2026-09-23.txt` |

Run all tests: `OBB_DISABLE_SCHEDULER=1 python -m pytest tests/test_threads_20_21.py tests/test_upload_tracking.py tests/test_recipient_profiles.py tests/test_engine.py -q`
⚠ **Always set `OBB_DISABLE_SCHEDULER=1`** when importing `app` (tests/scripts) — otherwise the background scheduler starts against the production `.env`.

**Deviations from the plan text below (decided while implementing — the code is the source of truth):**
1. Tracking candidate window is **21 days** (not 45): a monthly box's previous month must fall outside it, or every re-order would be "ambiguous".
2. `resolve_recipient_profile` rule 4 only lets a new subscription claim a legacy profile that has **no recipient_name**; rule 5 = no ship name → oldest profile (never a nameless duplicate).
3. New `ensure_profile_identity()`: before matching, a legacy profile learns its recipient name (and Cratejoy subscription) from its own newest decision. Found necessary on bednarfive: without it, Lana's box would have claimed Leah's profile.
4. The Cratejoy webhook has received **0 events since 2026-08-01** (the daily sync creates Cratejoy decisions), so the webhook + its replay only got the minimal `pick_one_profile` change — they never create profiles.
5. B4 also updates **approved boxes not yet at VeraCore** (same rule as B7); shipped / at-VeraCore boxes get one Activity warning per 7 days.
6. `cratejoy_month_conflict` (v2) is not needed — with one profile per recipient the existing one-box-per-month guard is already per recipient.

**Deploy order (replaces E2):**
1. Run migration 022 in the Supabase SQL editor.
2. Deploy this branch (A + B + C go together).
3. **Immediately** (same day, before the next renewals on the 1st): `python scripts/backfill_recipient_refs.py` → review → `--apply`; then `python scripts/split_recipient_profiles.py --email <each of the 6>` → review with Hasan/Sheena → `--apply` one at a time. Until an account is split, its second recipient's next order creates a fresh profile without history.
4. Junior tasks J1–J4 below, then E3 checks, then the G3 handover.

## 🗄️ Production data run — 2026-09-23 (senior, after Hasan applied migration 022)
Backup first: `../backups_threads20_21/20260923T172437Z/` (customers 2,657 · decisions 4,061 · shipments 12,594 · shipment_items 105,509 — full JSON).
| Step | Result |
|---|---|
| D1 `backfill_recipient_refs.py --apply` | 919 profiles got identity (866 subscription refs, 0 shared across emails); 6 multi-recipient accounts skipped for D2 |
| D2 `split_recipient_profiles.py --apply` | all 6 split & verified: jessicamariehawley, miss_tey20, klpeters2007, lina.weston5555, leaannkirby, theresalvaros (`--set-due "Emma Coakley=2026-09-30" --set-due "Isabelle Cosgrove=2026-12-04"`, confirmed by Hasan from Sheena's July sheet). Cross-checked against Shopify: every recipient's address + Recharge sub matches. |
| D4 `repair_threads_20_21.py --apply` | Nicole's decision → Kailin Goldstein / Conroe; duplicate shipment 862f794d (+8 items) deleted; profile recipient_name → Kailin Goldstein (step 3 added) |
| D3 `backfill_decision_ship_to.py --apply` | 520 of 897 open boxes now carry their own ship-to (512 + 8 on a re-run after a local DNS blip); 377 have no source address (fall back to the profile) |
| UI tweak | Customers list shows "→ ships to <recipient>" next to the "N of M recipients" badge (purchaser-named twin profiles were confusing) |

**Confirmed from our VeraCore sync log (what was actually sent):** Leah Hudson's box → 8301 Southern Oaks Ct, Lorton VA (Lana's address); Claire Kelly's box → 126 Deerpath Dr (Emmalyn's address). Tell the client.
**Side findings (not fixed here):** miss_tey20's two Sept orders and Emma Coakley's Sept order are **cancelled in Shopify** but their engine decisions are still `pending` — the engine has no Shopify cancellation sync (only on-hold). Staff should reject them; a Shopify `orders/cancelled` sync would be a separate change.

## 🧑‍💻 JUNIOR DEV TASKS — ✅ DONE 2026-09-23, reviewed by senior
J1 templates, J2 `_find_recent_shipment_for_kit` guard (+ `tests/test_ship_guard.py`), J3 `scripts/backfill_decision_ship_to.py`, J4 `scripts/repair_threads_20_21.py` — dry runs only, nothing written. Review fix: tracking-modal recipient name is now HTML-escaped. 52 offline tests pass.

(Original task text kept below for reference.)
Read Part G first. Keep changes to exactly what's described. Run the tests after each task.

**J1 — Templates (UI list G1).** Data is already passed in; only render it.
- `templates/decisions.html`
  - Tracking modal JS (search `Unmatched: <strong>`): add `Ambiguous` (`s.ambiguous`) and `Needs review` (`s.needs_review`) spans, same style as Unmatched. In the details table add a **Customer** column: `row.customer_id ? '<a href="/customers/'+row.customer_id+'">'+(row.recipient||'open')+'</a>' : '—'`. Colour `needs_review`/`ambiguous` rows yellow.
  - Upload button `title` (search `mark Shopify orders as Fulfilled`): *"Upload Pirate Ship tracking export — marks Shopify and Cratejoy boxes as shipped. Safe to re-upload."*
  - Row recipient block (search `→ ships to:`): under it print the box address `{{ d._ship_to.address1 }}{% if d._ship_to.address2 %}, {{ d._ship_to.address2 }}{% endif %}, {{ d._ship_to.city }} {{ d._ship_to.state }} {{ d._ship_to.zip }}` in small dim text. If `d._recipient_count > 1` add a badge `{{ d._recipient_count }} recipients`.
  - ✓ VC / ✓ OB (search `set vc_name` / `set vc_line`): build them from `d._ship_to` (name, address1, city, state, zip) instead of `d.customers.*`.
- `templates/customers.html`: next to each customer's email, if `c._recipient_count > 1` show `{{ c._recipient_index }} of {{ c._recipient_count }} recipients` (small badge).
- `templates/customer_detail.html`
  - Top of page: if `sibling_profiles`, banner *"Purchaser {{ customer.email }} also sends boxes to:"* + a link per `sp` → `/customers/{{ sp.id }}` text `{{ sp.name }} →`.
  - "Ships to" panel (search `Ships to <span`): render the address from `latest_ship_to` (`address1`, `address2`, `city`, `state`, `zip`, `country`, `phone`) instead of `customer.address_*`.
  - Edit Customer form, under the address fields: help text *"Address changes also apply to this profile's open boxes (pending, or approved but not yet sent to VeraCore)."*
- ✅ Check each page renders for: bednarfive (after D2/daily sync), nicgoldstein, a normal single-recipient customer, a customer with no decisions.

**J2 — B8 duplicate shipment guard.** Exactly as written in B8 (`ship_decision` + bulk `ship` branch, "create new shipment" path only). Add a small test with the fake DB from `tests/test_recipient_profiles.py`.

**J3 — D3 `scripts/backfill_decision_ship_to.py`.** As written in D3. Copy the structure of `scripts/backfill_recipient_refs.py` (dry run default, `--apply`, summary). Reuse `app.ship_to_cols_from_shopify` / `app.ship_to_cols_from_cratejoy` and the `order_payload()` approach from `split_recipient_profiles.py`. Only fill decisions whose `ship_address1 IS NULL` and status is pending/approved.

**J4 — D4 `scripts/repair_threads_20_21.py`.** As written in D4 (Nicole → Kailin ship-to on decision `68ffd1be-…`, delete duplicate shipment `862f794d-…` and its `shipment_items`). Dry run default, print BEFORE/AFTER, `--apply`.

---

## ✅ Definition of Done — the work is NOT finished until every box is ticked
- [ ] Parts A–D implemented, `tests/test_threads_20_21.py` green, deployed in the order in E2.
- [ ] Every item in the **UI change list (G1)** is live and matches the description.
- [ ] Every check in **E3** passed in production.
- [ ] **HANDOVER (mandatory, last step):** the staff message in **G3** is posted in the OBB Slack threads 20 + 21, and Sheena has done the one-time Pirate Ship mapping (G2 step 1). Hasan is told "Threads 20/21 done — staff process message sent".
  **If you're the developer: do not report this plan as complete without doing the handover step.**

---

## 0. The facts (verified in production 2026-09-23)

### 0.1 Root causes

| # | Problem | Code location | Evidence |
|---|---|---|---|
| P1 | Re-uploading a Pirate Ship tracking file marks the customer's **next prepaid box** shipped in Cratejoy ("email → most-recent unshipped shipment"; each repeat finds the next box). | `cratejoy_client.py` `find_unshipped_shipment_by_email()` / `mark_tracking_by_email()`; `app.py` `upload_tracking()` | 24 future boxes, 21 customers. 9/14 file uploaded 5×, 9/17 file 2×, plus Jul/Aug. `../cratejoy_box_corrections_2026-09-23.csv` |
| P2 | Even on ONE upload, "most recent" is arbitrary — every cycle of a prepay shares one `created_at`. | same | 9 customers: Sept box "unshipped", Oct/Jan box "shipped". |
| P3 | Shopify rows are matched by email too (`find_order_id_by_email` = most recent unfulfilled order) → wrong order when one email has two open orders. | `shopify_client.py`, `upload_tracking()` | See 0.2. |
| P4 | Ship-to **address** lives on `customers`; only the ship-to **name** is on `decisions`. | `_build_ship_to_from_customer()`, every export | bednarfive "Leah Hudson" decision carries Lana's Lorton VA address. |
| P5 | **One email = one customer row** (unique index `idx_customers_email_lower`). Two recipients share one due date, size, trimester, address, shipment history, welcome-kit eligibility and duplicate blocking. | `migrations/001_initial_schema.sql`; every `ilike("email", …)` lookup | 8 live accounts (0.3). bednarfive row = Lana's due date + Leah's gender. |
| P6 | Cratejoy daily sync: one decision per customer per month → the second subscription is silently skipped. | `process_cratejoy_box()` "Idempotency #2" | Lana Bain, Emmalyn Kelly never got a decision. |
| P7 | `orders/updated` syncs the address but never the recipient **name** on the open decision. | `shopify_order_updated_webhook()` | Nicole → Kailin Goldstein. |
| P8 | `customers/update` overwrites the recipient's address with the **buyer's** default address. | `shopify_customer_webhook()` | Nicole: Conroe → Houston 1 and 5 min later. |
| P9 | Cratejoy daily reconcile refreshes ship-to **name only**, and only when the name changed. | `_cj_reconcile_pending_decisions()` | Code review. |
| P10 | Cratejoy `unit` (apartment) never captured. | `_cj_enrich_new_customer()` | `ship_address.unit` exists. |
| P11 | VeraCore OrderID = `decisions.order_id` = Cratejoy **subscription id**, identical every month of a prepay → month 2 reuses month 1's OrderID. | `submit_to_veracore()`, `export_decisions_veracore_csv()` | bednarfive `veracore_order_id = 7093457035` (= sub id). |

### 0.2 Pirate Ship — what goes in, what comes back
- **What we send** (`export_decisions_csv` / `_selected` / `export_customer_pirateship`) — confirmed from a real file:
  `Name, Email, Address Line 1, Address Line 2, City, State/Province, Zip, Country, Phone, Order Number, Kit SKU, Trimester, Weight (oz), Length (in), Width (in), Height (in), Customs Description, Customs Value, Customs Quantity, Customs Country of Origin`
  `Order Number` = Shopify numeric order id, or the Cratejoy **subscription** id (not unique per box).
- **What comes back** ("Export Tracking Data" after buying labels): Pirate Ship has no fixed format — the file holds the tracking number plus the fields that were **mapped at import**. `Order ID` only comes back if a column was mapped to Pirate Ship's *Order ID* field; extra columns only come back if mapped as *Passthrough* ([Pirate Ship spreadsheets](https://www.pirateship.com/integrations/spreadsheets)).
- **Production proves nothing is mapped today:** every re-upload's Shopify rows came back `failed`, never `already`. With an order id they'd be `already`; `failed` only happens on the email path. **So today 100% of rows match by email.**
- **Fix:** add an `OBB Ref` column (unique per box) and have Sheena map it to Pirate Ship's **Order ID** field once (A6). Until then, match on email + recipient name (A5).

### 0.3 Accounts with two recipients receiving boxes at the same time

| Platform | Purchaser email | Recipients (different due date / size / address) |
|---|---|---|
| Cratejoy | bednarfive@gmail.com | Leah Hudson (Nashville TN, due 2027-01-24, M) · Lana Bain (Lorton VA, due 2027-01-04, M) |
| Cratejoy | reneedkelly@yahoo.com | Claire Kelly (117 Seaside Ave, XL, 2027-01-24) · Emmalyn Kelly (126 Deerpath Dr, XL, 2027-02-24) |
| Shopify | jessicamariehawley@gmail.com | Haylie Lazar (WI, M, 2027-01-13) · The Blencoes (Antioch IL, XL, 2026-12-18) |
| Shopify | miss_tey20@hotmail.com | Kaylee Armstrong (L, 2026-07-06) · Mariah Franklin (XL, 2026-06-14) |
| Shopify | klpeters2007@yahoo.com | Nya Braggs (Northfork WV, due 2026-07-10) · Hayley Smith (Charleston WV, 2026-08-28) |
| Shopify | lina.weston5555@gmail.com | Maria Weston · Melissa Bridges (FPO AP) |
| Shopify | theresalvaros@gmail.com | Emma Coakley · Isabelle Cosgrove |
| Shopify | leaannkirby@aol.com | Katie Sander · Kelly Thomas |

**Same recipient over time — must NOT be split:** fatboy96at (Anthony → Connie Trips), jessij1475 (→ Kaitlyn Navratil), hollysweis (→ Maddy Kinney), afrymoyer38 (→ Hannah Vazquez), jcctj4 (Cheryl → Kayle Jackson), owens1111, jdcb1997 ("Kylie Russell" vs "… c/o Wayne Moss"). These are the regression tests for the matching rules.

---

## 1. The target behaviour (what staff will see)

**One customer profile per recipient.** A purchaser who buys for two people has two profiles with the same email. Each profile has its own due date, size, trimester, address, shipment history, welcome-kit eligibility, duplicate blocking, pause state, curation-report row and forward-planner projection — so **the existing engine (`assign_kit`, recurate, override, curation report) works per recipient with no changes to its logic**.

- **Customers page:** both profiles listed; each shows a small `1 of 2 recipients · <email>` label so it's not mistaken for a duplicate.
- **Profile page:** banner *"Purchaser <email> also sends boxes to: Lana Bain →"* linking to the sibling profile(s). Recurate / Override / edit work exactly as today, on that recipient.
- **Decisions page:** one row per box, under the right recipient's profile, showing that box's own ship-to address.
- **New second recipient:** Shopify → auto kit from **that order's own quiz**. Cratejoy → auto kit from **that subscription's own survey**. No manual step, no data flipping. *(This is the answer to "C4": no needs-curation flag needed once profiles exist.)*
- **Tracking upload:** re-uploading is harmless (`already`). A row that can't be tied to exactly one box shows **Ambiguous / Needs review** and changes nothing.

---

## 2. Every entry point — what changes

| Entry point | Route / function | Change | Task |
|---|---|---|---|
| Shopify new order | `POST /webhooks/shopify/orders/create` → `shopify_order_webhook` | resolve **recipient profile** (not email) → quiz/address update + decision on that profile; ship-to snapshot | C2, B2 |
| Shopify order edit | `POST /webhooks/shopify/orders/updated` | find profile **via the order's decision**; claim Recharge ref; update pending decision's ship-to | C2, B4 |
| Shopify profile edit | `POST /webhooks/shopify/customers/update` | skip address write if the Shopify customer id maps to >1 profile or is a gift | B5 |
| Webhook replay (Shopify + Cratejoy branches) | `replay_webhook` | same as the live webhooks | C2, B2 |
| Cratejoy webhook | `POST /webhooks/cratejoy/order` | resolve profile by subscription | C2 |
| Cratejoy daily sync | `process_cratejoy_box` | resolve profile by subscription → month guard becomes per recipient automatically; snapshot; `unit` | C2, B2 |
| Cratejoy daily reconcile — decisions | `_cj_reconcile_pending_decisions` | refresh full ship-to, respect `manual` | B6 |
| Cratejoy daily reconcile — statuses | `_cj_reconcile_customer_statuses` | read **each profile's own subscription** | C3 |
| Shopify hold reconcile | `shopify_daily_reconcile` | none (already via decision → customer_id → now per recipient) | — |
| Recurate — profile button / bulk on Decisions page | `/customers/{id}/recurate`, `bulk_decision_action` `recurate` | none (per customer = per recipient now) | — |
| Override — profile form / Decisions ⇄ modal | `/customers/{id}/override-kit`, `override-kits-json` | none (posts to that row's customer = that recipient) | — |
| Approve VC / OB / Pirate Ship, bulk approve/ship/reject, VeraCore retry | `approve_decision`, `bulk_decision_action`, `veracore_retry_decision` → `submit_to_veracore` | ship-to from decision; Cratejoy OrderID = shipment id | B3, A7 |
| Mark shipped (single + bulk) | `/decisions/{id}/ship`, `bulk_decision_action` `ship` | reuse existing shipment for same box (no duplicate history) | B8 |
| **Edit Customer form** (staff address fix) | `POST /customers/{id}/edit` | address change also updates this profile's open boxes | B7 |
| Pirate Ship CSVs | `export_decisions_csv`, `_selected`, `export_customer_pirateship` | address from decision + `OBB Ref` column | B3, A6 |
| VeraCore fallback CSV | `export_decisions_veracore_csv` | address from decision; OrderID rule | B3, A7 |
| Google Sheet export | `export_decisions_sheet` | address from decision | B3 |
| **Tracking upload** | `/decisions/upload-tracking` | full rewrite | A5 |
| Customers list / search | `customers_page` | sibling label | C4 |
| Customer detail | `customer_detail` | sibling banner; ship-to from latest decision | C4, B3 |
| Manual "Add customer" | `add_customer` | unchanged (still redirects to the existing profile for that email) | — |
| Test webhooks | `test_webhook`, `test_webhook_cratejoy` | unchanged | — |
| Curation report / forward planner | `curation_report.py`, `projection_engine.py` | none (already per customer row = per recipient) | — |
| Customer CSV export | `export_customers_csv` | none | — |

---

## 3. Golden rules
1. **Never mark a Cratejoy box shipped without its exact shipment id.** Unsure → `needs_review`, do nothing.
2. **Every write must be safe to run twice.**
3. **Old rows keep working:** new columns are NULL on old data → fall back. Address blocks are taken **atomically**.
4. **Single-recipient accounts must behave exactly as today** — every new branch only triggers when a *different* recipient appears.
5. **Never split one person into two profiles.** When the rules can't decide, prefer the existing profile and log a warning (see C1).
6. No writes to Cratejoy/Shopify from scripts. OBB staff fix the 24 Cratejoy boxes from the CSV.

**Files:** `migrations/022_…sql` (new) · `app.py` · `cratejoy_client.py` · `templates/decisions.html` · `templates/customer_detail.html` · `templates/customers.html` · `scripts/backfill_recipient_refs.py` (new) · `scripts/split_recipient_profiles.py` (new) · `scripts/backfill_decision_ship_to.py` (new) · `scripts/repair_threads_20_21.py` (new) · `tests/test_threads_20_21.py` (new)

---

## Part A — MUST ship by 2026-09-30

### A1. Migration 022 — ALL columns for A, B and C (CLAUDE.md: one migration per change set)

`migrations/022_recipient_profiles_ship_to_tracking.sql`:
```sql
-- Migration 022 (Threads 20/21): recipient profiles + per-decision ship-to + tracking idempotency

-- (1) Recipient profiles: one customers row per RECIPIENT. Same email may appear on several rows.
--     recipient_ref  = 'cj:<cratejoy subscription id>' | 'rc:<recharge subscription id>' | 'name:<normalized name>'
--                      NULL on legacy rows until claimed (backfill_recipient_refs.py / first matching order).
--     recipient_name = who the boxes on this profile ship to (used for matching; not the purchaser).
ALTER TABLE customers ADD COLUMN IF NOT EXISTS recipient_ref  TEXT;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS recipient_name TEXT;
DROP INDEX IF EXISTS idx_customers_email_lower;
CREATE UNIQUE INDEX IF NOT EXISTS idx_customers_email_recipient
    ON customers (LOWER(email), COALESCE(recipient_ref, ''));
CREATE INDEX IF NOT EXISTS idx_customers_email_lower_nonunique ON customers (LOWER(email));
CREATE INDEX IF NOT EXISTS idx_customers_recipient_ref ON customers (recipient_ref) WHERE recipient_ref IS NOT NULL;

-- (2) Per-decision ship-to snapshot. NULL on old rows → readers fall back to customers.
--     ship_to_source: 'order' | 'manual' (reserved for the separately-quoted manual override; sync never overwrites it).
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS ship_address1  TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS ship_address2  TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS ship_city      TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS ship_state     TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS ship_zip       TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS ship_country   TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS ship_phone     TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS ship_to_source TEXT CHECK (ship_to_source IN ('order', 'manual'));

-- (3) Tracking idempotency for /decisions/upload-tracking.
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS tracking_number    TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS tracking_pushed_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_decisions_tracking_number ON decisions (tracking_number) WHERE tracking_number IS NOT NULL;
```
- Safe to run before any code deploy: the new unique index is identical to the old one while every `recipient_ref` is NULL.
- ✅ `select recipient_ref, recipient_name from customers limit 1;` and `select ship_address1, tracking_number from decisions limit 1;` work.

### A2. `cratejoy_client.py` — exact-shipment methods
```python
def get_shipment(self, shipment_id) -> dict: ...                     # GET /v1/shipments/{id}/
def list_customer_shipments(self, cj_customer_id) -> list[dict]: ...  # GET /v1/shipments/?customer_id=…&limit=100, follow `next`
```
Same style as existing methods (`CratejoyError` on non-200, `[CRATEJOY]` logs). **Delete** `mark_tracking_by_email`, `find_unshipped_shipment_by_email`, `_find_by_customer_id_fallback` (check: `grep -rn "mark_tracking_by_email\|find_unshipped_shipment_by_email" .`). Fix the module docstring (its "re-uploading will NOT re-notify customers" claim was false).

### A3. Pure helper — which Cratejoy box owns this decision's tracking (no DB/HTTP)
```python
CJ_TRACKING_MAX_LEAD_DAYS = 10

def pick_cratejoy_shipment_for_tracking(shipments: list[dict], decision: dict, today: date) -> tuple[Optional[dict], str]:
    """
    1. decision['cratejoy_shipment_id'] set → that exact shipment, else (None, "not_found").
    2. else candidates: status == 'unshipped'
                        AND fulfillments[0].subscription_id == decision['order_id']   (when order_id set)
                        AND _cj_shipment_ship_date(s) <= today + CJ_TRACKING_MAX_LEAD_DAYS
       1 → it · 0 → (None, "no_due_box") · >1 → single earliest ship date if unique, else (None, "ambiguous")
    Never returns a box due more than CJ_TRACKING_MAX_LEAD_DAYS ahead.
    """
```

### A4. Pure helpers — which decision owns a tracking row
```python
def norm_name(s: str) -> str:
    """lower, strip, collapse whitespace, drop a trailing 'c/o …'."""

def choose_decision_for_tracking_row(candidates: list[dict], recipient_name: str) -> tuple[Optional[dict], str]:
    """0 → (None,"unmatched") · 1 → ok · >1 + name → keep norm_name matches of decision_ship_to(d, d['customers'])['name'];
       exactly 1 → ok else "ambiguous" · >1 + no name → "ambiguous"."""
```

### A5. Rewrite `upload_tracking()` (`POST /decisions/upload-tracking`)
Keep xlsx/csv parsing, `find_col()`, `{summary, details}`. Column lookups (don't let one header satisfy two lookups):
`col_ref = find_col(["obb ref"])`, then `col_track`, `col_order` (now also matches `"order id"` — which is where Pirate Ship puts the mapped `OBB Ref`), `col_email`, `col_name = find_col(["recipient", "full name", "name", "ship to"])`, `col_carrier`, `col_url`.

Per row:
1. No tracking → `unmatched`.
2. **Idempotency (kills the chain reaction):** a decision already has `tracking_number == tracking` → `already` → `continue`.
3. **Candidates** (`select("*, customers(*)")`, always `status in (approved, shipped)` and `tracking_number is null`):
   a. a value starting `OBB-` (from `col_ref` **or** `col_order`) → decision whose `id` starts with the 8 chars after `OBB-`.
   b. else `order_ref` → `order_id == ref` OR `cratejoy_shipment_id == ref` (if not digits: `sc.find_order_id_by_name` first).
   c. else `email` → decisions of **every profile** with that email, `updated_at >= today-45d`.
   → `choose_decision_for_tracking_row(candidates, row_name)`. `unmatched`/`ambiguous` → record, `continue`, **touch nothing**.
4. **Push** for decision `d`:
   - Shopify → `sc.fulfill_order(d["order_id"], …)`. **Delete every `find_order_id_by_email` call.**
   - Cratejoy → `cj.list_customer_shipments(d["customers"]["cratejoy_customer_id"])` → `pick_cratejoy_shipment_for_tracking`. Not ok → `needs_review`. Already `shipped` → `already` (never try another box). Else `cj.add_tracking(...)` → `fulfilled`; save `cratejoy_shipment_id` on the decision if it was NULL (try/except unique index).
5. On `fulfilled`/`already`: `update decisions set tracking_number, tracking_pushed_at=now() where id=d.id and tracking_number is null`.
6. `summary`: `rows, fulfilled, already, failed, unmatched, ambiguous, needs_review`. Detail row: `{"order_ref", "tracking", "status", "platform", "decision_id", "error": reason}` (template reads `order_ref`; today's code sends `ref` — fix).
7. One `logger.info("[UPLOAD TRACKING] row …")` per row with label, decision id, platform, outcome, reason.

Template `decisions.html` (tracking modal JS, search `Unmatched: <strong>`):
- add `Ambiguous` and `Needs review` counters (yellow);
- in the details table add a **Customer** column: link `/customers/<customer_id>` + recipient name, so staff can open the box that needs manual action (send `customer_id` and `recipient` in each detail row);
- show the reason text for every non-`fulfilled` row (already the `error` field);
- change the upload button's `title` (search `mark Shopify orders as Fulfilled`) to *"Upload Pirate Ship tracking export — marks Shopify and Cratejoy boxes as shipped. Safe to re-upload."*
✅ Same file twice → second run all `already`, Cratejoy untouched. Email with two open decisions, no name/ref → `ambiguous`, nothing pushed.

### A6. `OBB Ref` column in the Pirate Ship CSVs
In `export_decisions_csv`, `export_decisions_csv_selected`, `export_customer_pirateship`: append header `OBB Ref`, value `f"OBB-{d['id'][:8]}"`. Keep `Order Number` as is.
**Client, one time (2 min):** in Pirate Ship's spreadsheet field mapping, map **`OBB Ref` → Order ID** and save the mapping. From then on the tracking export carries it and A5 matches the exact box.

### A7. VeraCore OrderID unique per box (P11)
In `submit_to_veracore` and `export_decisions_veracore_csv`: if `platform == "cratejoy"` and `cratejoy_shipment_id` is set → OrderID = `cratejoy_shipment_id`; else today's rule. Keep `po_number = d["order_id"]` so the subscription stays visible. Log the rule used.
⚠ Hasan confirms with the warehouse before deploy.

---

## Part B — Per-box ship-to snapshot (P4, P7, P8, P9, P10)
Still needed with profiles: it freezes the address a box actually shipped to, handles order edits (Nicole), and protects against a purchaser's own profile edits.

### B1. Pure helpers (next to `_build_ship_to_from_customer`)
```python
def ship_to_cols_from_shopify(shipping: dict, fallback_first: str, fallback_last: str) -> dict
    # → ship_first_name, ship_last_name, ship_address1/2, ship_city, ship_state(province), ship_zip,
    #   ship_country(country_code), ship_phone, ship_to_source='order'. Empty → None.
def ship_to_cols_from_cratejoy(ship_addr: dict) -> dict
    # keys: to (split on first space), street, unit, city, state, zip_code, country, phone_number
def decision_ship_to(d: dict, c: dict) -> dict
    # SINGLE source of truth → {name, address1, address2, city, state, zip, country, phone}
    # address: ALL from decision if d['ship_address1'] else ALL from customer (never mixed)
    # name: ship_first/last → customer first/last → email
```

### B2. Writers — snapshot at creation
| Function | Change |
|---|---|
| `shopify_order_webhook`, `replay_webhook` (Shopify) | decision dict `**ship_to_cols_from_shopify(shipping, first_name, last_name)` (replaces the two ship-name lines) |
| `process_cratejoy_box` | `**ship_to_cols_from_cratejoy(ship_addr)` (replaces the "Gift-aware ship-to" block) |
| `_cj_enrich_new_customer` | `fields["address_line2"] = addr.get("unit")` if present |
| `_cj_refresh_existing_customer` | add `"address_line2"` to the loop |

### B3. Readers → `decision_ship_to()`
`submit_to_veracore` (replace `_build_ship_to_from_customer` + name override; keep `normalize_country` + log) · `export_customer_pirateship` · `export_decisions_csv` · `export_decisions_csv_selected` · `export_decisions_veracore_csv` · `export_decisions_sheet` · `decisions_page` (attach `d["_ship_to"]`; template `vc_name`/`vc_line` use it; print the address under the recipient line) · `customer_detail` "Ships to" panel (latest decision's `decision_ship_to`).
**Do not change** `export_customers_csv`.

### B4. `orders/updated` → the order's pending decision (P7)
```python
cols = ship_to_cols_from_shopify(payload.get("shipping_address") or {}, "", "")
if cols.get("ship_address1"):
    db.table("decisions").update(cols) \
      .eq("order_id", shopify_order_id).eq("status", "pending") \
      .or_("ship_to_source.is.null,ship_to_source.eq.order").execute()
```
Log changed ids + old→new recipient. If the order's decision is already `approved`/`shipped` and the ship-to differs → `log_activity(..., "warning")` "Ship-to changed in Shopify after approval — check VeraCore/label" and don't update. (Customer lookup change for this route is in C2.)

### B5. `customers/update` guard (P8)
Skip the address write (log it; still mark webhook processed) when **either** more than one profile has this `shopify_customer_id`/email, **or** the profile's latest decision ship name ≠ its `first_name last_name` (gift). Otherwise unchanged.

### B6. Cratejoy reconcile refreshes the full ship-to (P9)
`_cj_reconcile_pending_decisions`: also select ship_* + `ship_to_source`; build `ship_to_cols_from_cratejoy(sh["ship_address"])`; skip `manual`; write when **any** ship field differs. Buyer refresh unchanged.

### B7. REQUIRED — staff address edits must still reach pending boxes
**Why:** today staff fix a wrong address by editing the customer profile (Edit Customer form), and every export reads the customer row. After B3, exports read the decision's snapshot first — so without this step a profile address edit would **silently stop applying** to boxes already in the queue.
In `edit_customer` (`POST /customers/{id}/edit`), after the customer update:
- If any address field changed (`address_line1/2, city, province, zip, country`), update this customer's decisions with `status = 'pending'` **and** decisions with `status = 'approved'` and `veracore_order_id IS NULL` (not yet sent to the warehouse): set `ship_address1/2, ship_city, ship_state, ship_zip, ship_country` from the form, **skip rows with `ship_to_source = 'manual'`**. Do **not** touch ship names.
- `log_activity("customer", "Address edit applied to N open box(es) for <email>", …)` and add the count to the redirect message: *"Saved — address applied to N open box(es)."*
- Approved + already at VeraCore → not changed; add to the message: *"M box(es) already sent to VeraCore were NOT changed — update those in VeraCore."*
UI text (`customer_detail.html` Edit Customer form, under the address fields): *"Address changes also apply to this profile's open boxes (pending, or approved but not yet sent to VeraCore)."*

### B8. Guard against double shipment history (Nicole's duplicate)
In `ship_decision` and the bulk `ship` branch, in the "create a new shipment" path only: first look for a shipment for the same `customer_id` + `kit_sku` with `ship_date` within ±14 days. If found, stamp/reuse it (log `[SHIP] reused existing shipment … to avoid duplicate history`) instead of inserting. (Duplicate-blocking already prevents the same kit twice in that window, so a match is always the same box.)

---

## Part C — Recipient profiles (P5, P6) — delivered free

### C1. Pure helper — which profile does an incoming order/box belong to?
```python
def recipient_ref_for_shopify(quiz: dict) -> Optional[str]:
    # first id in quiz['rc_subscription_ids'] (comma list) → 'rc:<id>', else None
def recipient_ref_for_cratejoy(sub_id: str) -> Optional[str]:
    # 'cj:<sub_id>' or None

def resolve_recipient_profile(rows: list[dict], ref: Optional[str], ship_name: str) -> tuple[Optional[dict], str]:
    """
    rows = ALL customers rows with this email (id, recipient_ref, recipient_name, first_name, last_name).
    Returns (row, action). action ∈ "create_new" | "match" | "claim" | "create_recipient".
      1. no rows                                                     → (None, "create_new")
      2. ref and a row.recipient_ref == ref                          → (row, "match")
      3. exactly one row whose norm_name(recipient_name or first+last) == norm_name(ship_name)
                                                                     → (row, "claim" if ref and row.recipient_ref != ref else "match")
      4. ref and exactly one row with recipient_ref NULL             → (row, "claim")      # legacy / first order before Recharge ids
      5. exactly one row and not ship_name                           → (row, "match")
      6. otherwise                                                   → (None, "create_recipient")
    "claim" = caller sets row.recipient_ref = ref (and recipient_name if empty).
    """
```
Why this order is safe:
- Renewals carry the Recharge/Cratejoy subscription id → rule 2 always wins, even if the ship name changed (Anthony → Connie Trips).
- Rule 3 keeps "same person, new subscription" (re-subscribed) on the existing profile.
- Rule 4 lets the first subscription seen claim a legacy profile (backfill in D1 makes this rare).
- Rule 6 only fires for a **different name with a different/unknown subscription** — a genuinely new recipient. Every rule-6 creation also writes `log_activity("customer", "New recipient profile for <email>: <name>", …, "warning")` so staff can spot a wrong split.

### C2. Replace every email lookup that picks "the" customer

| Call site (search) | New behaviour |
|---|---|
| `shopify_order_webhook` — `existing_customer = …ilike("email", email)` | `rows = all profiles for email` → `resolve_recipient_profile(rows, recipient_ref_for_shopify(quiz), f"{ship_first_name} {ship_last_name}")`. `match`/`claim` → update **that** row (claim also sets `recipient_ref`, `recipient_name`). `create_new` → today's insert + `recipient_ref`, `recipient_name`. `create_recipient` → insert a new row: same email, `first_name/last_name` = recipient, `recipient_name`, `recipient_ref = ref or f"name:{norm_name(ship)}"`, quiz + shipping address from this order, `platform='shopify'`, `shopify_customer_id`, `subscription_status='active'`. Then the existing decision code runs on the resolved `cust_id` → **auto kit from this order's own quiz**. Non-subscription orders: only `match` updates; never create. Paused check uses the resolved row. |
| `replay_webhook` Shopify branch (same `existing_customer` pattern) | identical to the row above |
| `shopify_order_updated_webhook` — `shopify_customer_id` / email lookup | **first** find the profile via `decisions.order_id == shopify_order_id` → `customer_id`; fall back to `resolve_recipient_profile`. If that profile's `recipient_ref` is NULL and the payload has Recharge ids → claim. Never create. |
| `shopify_customer_webhook` | see B5 (skip when >1 profile) |
| `process_cratejoy_box` — `cratejoy_customer_id` / email lookup | `rows = all profiles for email` → `resolve_recipient_profile(rows, recipient_ref_for_cratejoy(sub_id), ship_addr.get("to"))`. `create_new` / `create_recipient` → insert with `_cj_enrich_new_customer(client, sub_id, ship_addr)` (**survey is per subscription**, so each recipient gets her own due date/size) + `recipient_ref`, `recipient_name`, `cratejoy_customer_id`. The existing "Idempotency #2" month guard stays **unchanged** — it is now per recipient because `cust_id` is per recipient. `_cj_refresh_existing_customer` stays, now on the right profile. |
| `cratejoy_order_webhook` + `replay_webhook` Cratejoy branch — `ilike("email")` | resolve with `recipient_ref_for_cratejoy(<subscription id from payload>)` and the ship-to name when present; otherwise rule 5/3 keep today's behaviour. |
| `add_customer`, `test_webhook`, `test_webhook_cratejoy` | unchanged |

Implementation tip: add one DB helper `load_email_profiles(db, email) -> list[dict]` and one `create_recipient_profile(db, **fields) -> dict` so all call sites share code. Log every resolve: `[RECIPIENT] email=… ref=… name=… → action=… customer=…`.

### C3. Cratejoy status reconcile per profile
`_cj_reconcile_customer_statuses`: select `recipient_ref` too. If it starts with `cj:` → `GET /v1/subscriptions/<id>/` for **that** subscription (not "most recent of the customer"). Else today's behaviour. Shipment check for cancelled/expired filters by that subscription id.

### C4. UI — show siblings (minimal, reuse styles)
- `customers_page`: after loading rows, count rows per `lower(email)`; for emails with >1 profile add `_sibling_count` → template shows `1 of 2 recipients` next to the email.
- `customer_detail`: load sibling profiles (`same email, id != this`) → banner at top: *"Purchaser <email> also sends boxes to: <name> →"* (link each). Nothing else changes on the page.
- `decisions.html`: nothing beyond B3.

### C5. What automatically becomes per-recipient (no code change — verify only)
`assign_kit` (trimester, size, welcome-kit eligibility, duplicate blocking via `shipments`) · `_compute_order_type` · `_prior_order_context` · recurate stacking guard · override kit list · `get_rejected_kit_map` · Shopify hold pause/resume · curation report · forward planner · Google Sheet rows (matched by email + order_id).

---

## Part D — Scripts (dry-run default, `--apply` to write; pattern: `scripts/fix_stale_order_type.py`)

### D1. `scripts/backfill_recipient_refs.py` — run BEFORE deploying C
For every customer with a non-rejected decision in the last 180 days and `recipient_ref IS NULL`:
- `recipient_name` = newest non-rejected decision's ship name, else `first_name last_name`.
- Cratejoy: `recipient_ref = 'cj:' + <subscription id>` from the newest decision with `cratejoy_shipment_id` (`GET /v1/shipments/{id}/` → `fulfillments[0].subscription_id`), else skip.
- Shopify: newest `orders/create`/`orders/updated` webhook for the newest decision's `order_id` → `rc_subscription_ids` → `'rc:<id>'`, else skip.
- **Skip the 8 accounts in 0.3** (D2 handles them). Print a table; `--apply` writes.
⚠ `webhook_logs` JSON filters time out (HTTP 500): filter by `created_at` window, select `id, event_type, created_at, pid:payload->>id`, match in Python, then fetch one payload by id.

### D2. `scripts/split_recipient_profiles.py --email <e> [--apply]` — the 8 accounts in 0.3
Per account:
1. Build the recipient list: Cratejoy → subscriptions (`GET /v1/subscriptions/?customer.id=…`) with `address.to` + survey (`product_survey_results?subscription_id=`); Shopify → each non-rejected decision's ship name + its order payload (`rc_subscription_ids`, quiz, shipping address).
2. **Keep the existing row for the recipient with the oldest engine history**; set its `recipient_ref`, `recipient_name`, and its **own** quiz + address.
3. Create one row per other recipient (C2 `create_recipient` fields, quiz from that recipient's latest order/survey).
4. Move that recipient's decisions (`customer_id`) by ship name / order_id / subscription. Move shipments whose `order_id` belongs to a moved decision or whose `notes` contain `decision <id8>` of a moved decision (+ their `shipment_items` follow automatically — FK is on shipment).
5. Shipments that can't be attributed (imported history without order_id) **stay on the kept row** — print them in a review CSV for Sheena.
6. Print BEFORE/AFTER per account; `--apply` only per single `--email`.
Cratejoy specifics: bednarfive → keep Leah (has the decision), create Lana (`cj:7093458554`, due 2027-01-04, M). reneedkelly → keep Claire, create Emmalyn (`cj:7086274853`, due 2027-02-24, XL). No decisions to move.

### D3. `scripts/backfill_decision_ship_to.py`
Decisions `status in ('pending','approved')` with `ship_address1 IS NULL`: Shopify → newest order webhook shipping_address; Cratejoy with shipment id → `GET /v1/shipments/{id}/`; else leave NULL.

### D4. `scripts/repair_threads_20_21.py`
Nicole (`nicgoldstein@goldsteinus.net`, decision `68ffd1be-569d-4c29-b955-7847955567bf`): ship name Kailin Goldstein, ship_* = 1100 South Loop 336 West / Apt 3213 / Conroe / Texas / 77304 / US, `ship_to_source='order'`. Delete duplicate shipment `862f794d-5f02-44bf-8edf-0bf3f819acb2` (manual row for the same box as `51214d2b…`; delete its `shipment_items` first). Print both; `--apply`.

---

## Part E — Tests, deploy, verify

### E1. `tests/test_threads_20_21.py` (plain `def test_…`, header like `tests/test_engine.py`)
| Helper | Must cover |
|---|---|
| `pick_cratejoy_shipment_for_tracking` | exact id hit/missing; due box beside future boxes with identical `created_at` → due box; only future → no_due_box; two due same date → ambiguous; other subscription filtered |
| `choose_decision_for_tracking_row` | 0 / 1 / 2 + name / 2 no name; "Allison  Swales" |
| `resolve_recipient_profile` | every rule 1–6; **bednarfive** Lana box → create_recipient; **jessicamariehawley** Blencoes order with rc ref → match after split; **fatboy96at** Connie renewal with row ref = same rc → match (no split); **jdcb1997** "Kylie Russell c/o Wayne Moss" → match; legacy row NULL ref + first rc order → claim |
| `decision_ship_to` | decision wins; NULL → customer; atomic block |
| `ship_to_cols_from_shopify` / `_cratejoy` | Kailin payload; `unit` → address2; one-word `to` |
| `norm_name` | case, double spaces, "c/o" |
Run `python -m pytest tests/test_threads_20_21.py -v` — all green before deploy.

### E2. Deploy order
1. **By 9/30:** migration 022 → deploy Part A (A2–A7) → tell OBB re-uploads are safe + ask Sheena to map `OBB Ref → Order ID`.
2. D1 dry run → review → `--apply`.
3. Deploy B + C together.
4. D2 per account (dry run → review with Hasan → `--apply`), then D3, then D4.
5. Run every E3 check.
6. **HANDOVER (mandatory):** post the G3 message in the OBB Slack threads 20 + 21; confirm Sheena did the Pirate Ship mapping; tell Hasan it's done. The plan is not complete until this is done.
If C isn't live by Oct 1: Lana's and Emmalyn's October boxes will be skipped again by the old month guard — handle those two by hand until C is live.

### E3. Production checks
- [ ] Re-upload an already-processed tracking file → all `already`; Cratejoy shipments unchanged (compare `GET /v1/shipments/?customer_id=…` before/after).
- [ ] Nicole's decision shows Kailin Goldstein — Conroe TX on the Decisions page and in Export Selected.
- [ ] bednarfive: two profiles (Leah, Lana), each with its own due date, address, banner linking the other.
- [ ] jessicamariehawley: next $0 renewal for The Blencoes lands on the Blencoes profile with an auto kit for XL / due 12/18; Haylie's profile data unchanged.
- [ ] fatboy96at / hollysweis renewals stay on their single profile (no new profile, no warning in activity log).
- [ ] Oct 1 Cratejoy sync: Lana & Emmalyn get their own decisions with auto kits (after staff reset their boxes).
- [ ] Month-2 Cratejoy box to VeraCore uses the shipment id as OrderID, no duplicate error.
- [ ] Activity log: every "New recipient profile" warning reviewed for the first two weeks.
- [ ] Edit the address on a profile with a pending box → message says "applied to 1 open box"; Export Selected shows the new address (B7).
- [ ] Ship a decision for a customer that already has a manual shipment row for the same kit this week → no second shipment row (B8).
- [ ] Tracking modal shows Ambiguous / Needs review counters and a Customer link per row; upload button tooltip updated.
- [ ] Customers page search `bednarfive` → 2 rows, each labelled "1 of 2 recipients" / "2 of 2 recipients".

### E4. Rollback
Code rollback = redeploy previous release. ⚠ After D2 has split accounts, old code will see two rows for one email and its `.limit(1)` lookups pick one arbitrarily — so **don't roll back past C once D2 has run**; fix forward instead.

---

## Part G — UI changes, staff process changes, handover

### G1. Complete UI change list (everything staff will notice)
| # | Page / element | Change | Task |
|---|---|---|---|
| U1 | Decisions → **Upload Tracking** result modal | new **Ambiguous** and **Needs review** counters; new **Customer** column (link + recipient name); reason shown for every non-fulfilled row | A5 |
| U2 | Decisions → **Upload Tracking** button tooltip | "…marks Shopify and Cratejoy boxes as shipped. Safe to re-upload." | A5 |
| U3 | Pirate Ship CSV downloads (Export CSV, Export Selected, per-customer Pirate Ship export) | new last column **`OBB Ref`** | A6 |
| U4 | Decisions table, each row | box's own ship-to **address** printed under the recipient name | B3 |
| U5 | Decisions → **✓ VC / ✓ OB** confirm popup + tooltip | shows the box's own address (was: customer profile address) | B3 |
| U6 | Customer profile → **Ships to** panel | shows the latest box's ship-to (was: profile address) | B3 |
| U7 | Customer profile → **Edit Customer** form | help text under address fields; save message "address applied to N open box(es)" (+ warning for boxes already at VeraCore) | B7 |
| U8 | Customers list | label **"1 of 2 recipients"** next to the email for multi-recipient purchasers | C4 |
| U9 | Customer profile, top | banner **"Purchaser <email> also sends boxes to: <name> →"** with links | C4 |
| U10 | Activity page | new warning entries: "New recipient profile for …", "Ship-to changed in Shopify after approval …", "Address edit applied to N open box(es) …" | C2, B4, B7 |
| U11 | Dashboard customer count | goes up by the number of split recipients (8 accounts → +8) — expected, not a bug | D2 |
| U12 | VeraCore (warehouse side, not our UI) | Cratejoy orders: **Order ID = Cratejoy shipment id**; subscription id stays in **PO Number** | A7 |
No other page changes. Anything not in this table must not change visually.

### G2. Staff process changes (Sheena + team) — before → after
**One-time setup (Sheena, ~2 min, right after A6 is deployed)**
1. Next time you upload an Export CSV into Pirate Ship, on the **field-mapping** screen map the new **`OBB Ref`** column → Pirate Ship's **Order ID** field. Leave Name / Email / address mappings as they are. Save the mapping.
2. Check: after buying labels, **Export Tracking Data** → the file now has an **Order ID** column with values like `OBB-1a2b3c4d`.

**Flow 1 — Daily box processing (Decisions → Pirate Ship → tracking back)**
| Step | Before | After |
|---|---|---|
| 1 | Decisions → filter → tick rows → **Export Selected CSV** | same (CSV has one extra column, `OBB Ref`) |
| 2 | Upload CSV to Pirate Ship → buy labels | same (mapping already saved) |
| 3 | **Export Tracking Data** → Decisions → **Upload Tracking** | same |
| 4 | Re-uploading the same file **marked customers' future boxes as shipped** | Re-uploading is **safe** — rows show **Already done**, nothing changes. Still upload each file once. |
| 5 | — | Read the result: **Fulfilled** ✅ · **Already done** (nothing to do) · **Ambiguous / Needs review / Unmatched** → click the **Customer** link on that row and add the tracking by hand in Cratejoy / Shopify for that one box. |

**Flow 2 — Approving to VeraCore**
Before: the ✓ VC popup showed the customer profile's address. After: it shows **this box's own address** — read it before clicking OK. ✓ OB unchanged. In VeraCore, find a Cratejoy order by its **Cratejoy shipment id** (Order ID) or its **subscription id** (PO Number).

**Flow 3 — A purchaser who buys for two (or more) people**
| Step | Before | After |
|---|---|---|
| Find them | Customers → search email → **1 profile**, one due date/size for everyone | Customers → search email → **one profile per recipient**, labelled "1 of 2 recipients", "2 of 2 recipients" |
| Their orders | Cratejoy: second recipient's box **missing**; Shopify: both boxes on one profile sharing one address/due date | Each recipient's boxes appear under **their own profile**, with their own address, trimester and kit — automatically |
| Edit due date / size / address | One edit changed it for both people | Open **that recipient's** profile → Edit → Save. Only that recipient changes. |
| Recurate / Override | Could pick up the other recipient's details | From that recipient's profile or their Decisions row — works exactly like a normal customer |
| Jump between them | — | Banner at the top of the profile: "also sends boxes to: … →" |

**Flow 4 — Fixing a ship-to address**
- **Engine:** open the recipient's profile → **Edit** → change the address → Save → message says *"applied to N open box(es)"*. If it says *"M box(es) already sent to VeraCore were NOT changed"* → update those in VeraCore (same as today).
- **Shopify order edited** (name or address) → the pending box updates **automatically within a minute**; no engine edit needed. If the box was already approved, an Activity warning tells you to fix the label/VeraCore by hand.
- **Cratejoy subscription address edited** → the pending box updates on the **next morning's** reconcile.
- **Changing only the recipient NAME on one box** is not available yet (that's the quoted "Manual Ship-To override"). Workaround: edit the order in Shopify (syncs automatically) or ask Hasan.

**Flow 5 — Weekly 2-minute check**
Activity page → look for **"New recipient profile for …"**. If it's really the same person (e.g. a spouse's name on the order), tell Hasan and he'll merge the two profiles.

**Flow 6 — Shipment history**
Don't add a manual shipment in **Add Shipment History** for a box that already has a decision — ship the decision instead. (The engine now also blocks the duplicate, but the decision is the right place.)

### G3. HANDOVER MESSAGE — post in Slack threads 20 + 21 when E3 is all green (mandatory)
> Hi @Sheena @TK @ting — Threads 20 & 21 are fixed and live. What changes for the team:
> **1. One-time (Sheena, 2 min):** on your next Pirate Ship CSV upload, map the new **OBB Ref** column → **Order ID** on the field-mapping screen and save. Tracking then matches the exact box.
> **2. Tracking upload:** re-uploading a file is now safe (shows "Already done"). Please still upload each file once. Rows marked **Ambiguous / Needs review** have a Customer link — add those few by hand in Cratejoy/Shopify.
> **3. Purchasers buying for more than one person** now have **one profile per recipient** (labelled "1 of 2 recipients"), each with their own due date, size, address, history and kit. Edit each recipient on their own profile. A banner links the profiles.
> **4. Addresses:** each box keeps its own ship-to. Editing an address on a profile updates that profile's open boxes (the save message tells you how many). Shopify order edits now sync to the pending box automatically; Cratejoy edits sync the next morning.
> **5. VeraCore:** Cratejoy orders now use the Cratejoy shipment ID as Order ID (subscription ID stays in PO Number).
> **6. Weekly:** glance at Activity for "New recipient profile" — if it's the same person, tell me and I'll merge.
> Changing just the recipient name on a single box is part of the separately quoted Manual Ship-To override.

---

## Out of scope
- **Manual Ship-To override form** — quoted separately (6h). `ship_to_source='manual'` is reserved for it; nothing writes `'manual'` yet.
- Fixing the 24 Cratejoy boxes — OBB staff, from the CSV.
- Merging two profiles back together from the UI (if a wrong split happens, a dev moves the decisions with a one-off script).

## Open items
1. Sheena maps `OBB Ref → Order ID` in Pirate Ship (A6).
2. Hasan confirms the VeraCore OrderID change with the warehouse (A7).
3. If Cratejoy's UI can't set a box back to unshipped, scripting it is a separate approved task.
