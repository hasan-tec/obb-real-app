# OBB Curation Engine — Change Log

**SOW baseline:** $1,500 fixed / 4 phases / 2 revisions per phase (§6)  
**Change Control clause:** SOW §8 — all new requests logged, estimated, and approved in writing before work starts.

---

## Revision Cap Summary

| Phase | Revisions Contracted | Revisions Used | Over Cap By |
|---|---|---|---|
| Phase 1 | 2 | ~8 | +6 |
| Phase 2 | 2 | ~7 | +5 |
| Phase 3 | 2 | 0 (in progress) | — |
| Phase 4 | 2 | 0 (not started) | — |

Items #1–18 below were delivered as goodwill. Going forward, SOW §8 Change Control applies to all new requests.

---

## Phase 1 — Delivered Changes (Goodwill / Out of SOW)

| # | Date | Requested By | Request | SOW In-Scope | Hours Est. | Status | Charged |
|---|---|---|---|---|---|---|---|
| 1 | Apr 8 | Tony (Meeting 1) | Auto-compute age rank from SKU prefix alphabetically | ❌ No | 4h | ✅ Delivered | $0 |
| 2 | Apr 8 | Ting (Meeting 1) | Google Sheets-style filters + clickable sort headers on all tables | ❌ No (SOW §7 #4) | 6h | ✅ Delivered | $0 |
| 3 | Apr 8 | Ting (Meeting 1) | Pirate Ship CSV export (Phase 3 item, delivered in Phase 1) | ❌ Phase 3 item | 5h | ✅ Delivered | $0 |
| 4 | Apr 8 | Sheena (Meeting 1) | "Small" (S) as a separate size variant in kit assignment logic | ❌ No | 3h | ✅ Delivered | $0 |
| 5 | Apr 8 | Sheena (Meeting 1) | Universal kit checkbox on kit create/edit form | ❌ No | 2h | ✅ Delivered | $0 |
| 6 | Apr 8 | Ting (Meeting 1) | Full historical data import: 1,603 customers / 7,093 shipments / 60,067 item records | Borderline (scale massively exceeded what was implied in SOW §4.2) | 12h | ✅ Delivered | $0 |
| 7 | Apr 8 | Sheena (Meeting 1) | Welcome kits treated as a separate concept from renewal kits | ❌ No | 4h | ✅ Delivered | $0 |
| 8 | Apr 8 | Sheena (Meeting 1) | Kit SKU edit fix (UI bug preventing SKU updates) | ✅ Bug fix | 1h | ✅ Delivered | $0 |

**Phase 1 revision count used: ~7–8 of 2 contracted.**

---

## Phase 2 — Delivered Changes (Goodwill / Out of SOW)

| # | Date | Requested By | Request | SOW In-Scope | Hours Est. | Status | Charged |
|---|---|---|---|---|---|---|---|
| 9 | Apr 15 | Ting (Meeting 2) | Customer CSV export with active filters | ❌ No | 3h | ✅ Delivered | $0 |
| 10 | Apr 15 | Ting/Sheena (Meeting 2) | Pirate Ship CSV default dims (10×7.5×4) + weight (48oz/3lb) | ❌ No | 1h | ✅ Delivered | $0 |
| 11 | Apr 15 | Ting (Meeting 2) | Bulk reject + bulk re-curate buttons on decisions page | ❌ No (SOW Phase 4 = single-row override only) | 5h | ✅ Delivered | $0 |
| 12 | Apr 15 | Ting (Meeting 2) | Item expiry date field → auto-flag as DO NOT USE in curation report | ❌ No | 4h | ✅ Delivered | $0 |
| 13 | Apr 15 | Ting (Meeting 2) | Active/past customer grouping by last shipment date | ❌ No (UI change) | 2h | ✅ Delivered | $0 |
| 14 | Apr 15 | Ting (Meeting 2) | Welcome-kit watchlist section in curation report | ❌ No | 3h | ✅ Delivered | $0 |
| 15 | Apr 15 | Ting (Meeting 2) | Forward planner as a separate dedicated UI tab | Logic in-scope; dedicated tab was extra | 4h | ✅ Delivered | $0 |
| 16 | Apr-May | Sheena (Slack) | Welcome-kit history import (separate data source, separate import script) | Borderline | 6h | ✅ Delivered | $0 |
| 17 | Apr-May | Sheena (Slack) | Item alternatives (pipe items) table for interchangeable product blocking | ❌ No | 3h | ✅ Delivered | $0 |
| 18 | May | Sheena (Slack) | Dashboard shows most recent decision instead of original (audit flow update) | ❌ No | 2h | ✅ Delivered | $0 |

**Phase 2 revision count used: ~7 of 2 contracted.**

---

## Outstanding — Client-Blocked Items (No Action From Hasan Until Client Delivers)

| # | Date | Requested By | Item | Blocker | Hasan's Position |
|---|---|---|---|---|---|
| B1 | Apr 15 | Ting/TK | Welcome kit history import (which customer got which WK over 24 months) | Sheena still manually compiling ~3,000 rows. Data not delivered. | Explicitly flagged as **separately chargeable** in Slack: "outside original scope, would require additional development, will scope and charge separately." |
| B2 | Apr | Hasan | Pipe items / item alternatives list (interchangeable products for duplicate blocking) | Sheena said she'd check — never provided the list. Item_alternatives table has 0 rows. | Cannot complete DO NOT USE accuracy without this. Blocked on client. |
| B3 | Apr | Sheena | Kit composition confirmation (OBB-AB-11 / OBB-AC-11 / OBB-BN-41 have very few items — are they correct?) | Sheena said she'd check. No reply received. | Waiting. |

---

## Phase 3 — Pending Change Requests (Approval Required Before Work Starts)

| # | Date | Requested By | Request | SOW In-Scope | Hours Est. | Approved | Charged |
|---|---|---|---|---|---|---|---|
| 19 | May 13 | TK (Slack) | Single-customer Pirate Ship CSV export | ❌ No | 2h | ✅ Delivered | $40 |
| 20 | May 13 | TK (Slack) | Expiry date extraction from VeraCore product description field | ❌ No | 7h | ✅ Delivered | $140 |
| 21 | May 13 | TK (Slack) | Kit quality/age detection — flag old kits for warehouse spot-check before shipping | ❌ No (new feature) | 4h | ⏳ Pending | $80 |
| 22 | May 13 | TK/Ting (Slack) | VeraCore shipping label creation via warehouse carrier account | ❌ No — Phase 3 covers warehouse order submission (AddOrder) only. EasyPost API recommended as the clean alternative, scoped separately. | 10–12h | ⏳ Pending | $200–$240 |
| 23 | May | Sheena (Slack) | Override dropdown shows only in-stock kits (currently shows all including zero-stock) | ❌ No — UI enhancement to the override flow; Phase 4 covers the override *logic*, not dropdown UX filtering. SOW §7 #4 excludes UI redesign beyond functional requirements. | 2h | ✅ Delivered (rolled into #26) | $0 — folded into #26 |
| 24 | May | Sheena (Slack) | Distinguish truly new orders from subscription continuations after payment issues | ❌ No — new dashboard indicator not in any phase scope. SOW §7 #1 (new features after sign-off) and §7 #4 (UI redesign) apply. | 3h | ⏳ Pending approval | $60 — change order required |
| 25 | May | Sheena (Slack) | Duplicate customer entries appearing in dashboard | ❓ Needs investigation — if system is creating duplicate records erroneously = bug (free, SOW §6). If it's a display/filter issue or data entry = change order. Pending client providing specific examples to reproduce. | 1h | ⏳ Pending investigation | $20 — $0 if confirmed bug, change order if not |

---

## Phase 3 — June 1 Meeting (Final QnA) Change Requests

Raised in the June 1 walkthrough with Tony + Sheena. Items #28–#29 are new VeraCore↔OBB sync features (SOW §7 new scope); #26 was delivered live during/after the call; #27 is the foundational reliability fix both sync features depend on.

| # | Date | Requested By | Request | SOW In-Scope | Hours Est. | Approved | Charged |
|---|---|---|---|---|---|---|---|
| 26 | Jun 1 | Tony (Meeting 3) | Override kit dropdown run through the assignment algorithm — show only kits **suitable** for the customer (correct trimester, matching size, no duplicate items, in stock), excluding the auto-assigned kit. Supersedes #23. | ❌ No — Phase 4 covers override *logic*, not dropdown filtering (SOW §7 #4) | 6h | ✅ Delivered | $120 |
| 27 | Jun 1 | Hasan (research) | VeraCore↔OBB SKU-matching hardening + `veracore_sku` mapping field on kit/item forms. **Prerequisite for #28 & #29.** Also fixes kits silently not syncing when OBB SKU ≠ VeraCore Offer ID (e.g. `WKC3` vs `OBB-WK-C3 Kits`) — those kits' stock never reconciled and drifted down. Implemented: `normalize_sku()` with dash/slash variants, matching updated in `run_inventory_sync` + `run_expiry_sync`, `veracore_sku` param on 4 add/edit routes, `needs_review` guard in `assign_kit` + override dropdown. | Partly bug (silent non-sync) / partly enhancement (UI field) | 6h | ⏳ Pending | $120 |
| 28 | Jun 1 | Tony (Meeting 3) | **VeraCore → OBB cancellation sync.** Nightly detect cancelled VeraCore orders (`GET /api/GetCanceledOrders`), flag the OBB decision as `cancelled`, and auto-remove the stale shipment history so duplicate-detection stays accurate. Fixes the shipment-history discrepancy seen on the call. Implemented: `get_canceled_orders()` in client, `run_cancellation_sync()` in sync engine, migration 014, scheduler block + manual trigger route. | ❌ No — new feature (SOW §7 #1) | 7h | ⏳ Pending | $140 |
| 29 | Jun 1 | Tony (Meeting 3) | **VeraCore → OBB new-offer auto-sync.** Nightly detect new VeraCore offers (`GET /api/Offers`) and auto-create OBB kits, deriving trimester / size / welcome-flag / age-rank from the SKU formula; flags unparseable SKUs for review. Eliminates the double-entry of creating each kit in both systems. Implemented: `parse_kit_attrs_from_sku()`, `run_offer_sync()`, migration 015, scheduler + manual route. Also included live API diagnostic (Jun 8) against Ting's tenant which uncovered and fixed 3 additional bugs: `inactive` field always evaluating true (was parsing `Status.Inactive` existence instead of `Indicator == 1`), `normalize_sku` missing `-KITS` dash-variant and slash combo cases, and `run_offer_sync` missing kit-type filter (would have created kit rows for all 981 active offers including items). SOAP BOM investigation completed — kit item composition is readable via `GetProduct` on `order.asmx` (implementation deferred pending client decision on scope). | ❌ No — new feature (SOW §7 #1) | 14h | ⏳ Pending | $280 |
| 30 | Jun 1 | Tony (Meeting 3) | **New vs renewal order indicator.** Visual badge on decisions page (and customer name column on customers page) showing New/Renewal. Filter dropdowns added to both pages. Logic: 0 prior shipments = New, otherwise = Renewal. | ❌ No — new UI indicator, not in any phase scope (SOW §7 #1 + §7 #4) | 2h | ✅ Delivered | $40 |
| 31 | Jun 1 | Tony (Meeting 3) | **Pirate Ship CSV export by row selection.** "Export Selected CSV" button in bulk action bar — tick decisions, click export, get CSV for only those rows. | ❌ No — enhancement to existing CSV export (SOW §7 #1) | 3h | ✅ Delivered | $60 |
| 32 | Jun 1 | Hassan (bug fix) | **Platform field missing on manual override decisions.** When staff used the kit override flow, `decisions.platform` was not inherited from the customer record — causing those decisions to be excluded from platform-filtered CSV exports. | ✅ Bug (SOW §6) | 1h | ✅ Fixed (commit 32c19b1) | $0 |

**June 1 batch subtotal (pending): ~32h** (#27 6h + #28 7h + #29 14h + #30 2h + #31 3h). #26 (6h) and #32 (bug) already delivered.

> ✅ **API shapes fact-checked (Jun 8 2026):** live diagnostic against Ting's tenant confirmed `GetCanceledOrders` field casing, offers `inactive` field structure, and inventory endpoint path. Three implementation bugs discovered and fixed during diagnostic. Quote is fixed-price.

---

## Notes

- **Bugs** (system not doing what was agreed) = fixed at no cost, no cap, do not count as revisions — per SOW §6.
- **Revisions** (changes to agreed spec) = 2 per phase. Revisions in excess of 2 are goodwill during active development. From Phase 3 onward, excess revisions require a change order per SOW §8.
- **New features** discovered after phase sign-off = new scope per SOW §7 and §12.
