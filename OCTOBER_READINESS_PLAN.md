# October Readiness Plan: Thread 25 bugs + September manual shipments

> **Written:** 2026-09-29 · **Deadline:** Tasks 1–2 live before the Oct 1 daily sync; Task 3 before the first October bulk run. Bugs only; quoted features are listed at the end and are not built here.
> **Read `CLAUDE.md` first.** Every script: dry run by default. `--apply` is run by **Hasan only**. Tests and scripts always set `OBB_DISABLE_SCHEDULER=1` **before** `import app` (otherwise the scheduler fires real jobs against production).
> One branch per task, a PR per task, and Hasan merges. Run the full suite before every PR:
> `OBB_DISABLE_SCHEDULER=1 .venv/Scripts/python -m pytest tests -q -p no:warnings`

---

## What was found (evidence, 2026-09-29)

| # | Client report | Verdict | Evidence |
|---|---|---|---|
| F1 | "Load September tracking to the engine" (Sheena) | **Real, urgent.** The sheet has no tracking: it's **292 Shopify T3/T4 orders shipped by hand** (kits CQ41 / CQ31). **288 of them are still `pending` in the engine**, 3 already have a September shipment and 1 has no decision. If they aren't recorded, October curation has no September history for them, so duplicate blocking is blind and the pending rows block their October box ("other pending decision exists"). A Cratejoy renewals `.xlsx` was also posted in Slack (not yet downloaded). | Google Sheet `1oCj9YDOHBF7aDHjCl2ij6Q__JV1TLJ9h9Y-9WfQrVdI` matched against `decisions` / `shipments` |
| F2 | Thread 25: BT-21 assigned far beyond its 3 units | **Bug.** `assign_kit` (app.py ~2753) only filters `quantity_available > 0`. Stock is decremented at **approve/ship**, not when a pending box is created, so every pending box sees the same 3 units. VeraCore sync then resets `quantity_available` to the warehouse count. | app.py ~2840 (`.gt("quantity_available", 0)`), ~8346 (approve decrement), veracore_sync.py ~163 |
| F3 | Thread 25: "Application Error" on bulk re-curate | **Bug (timeout).** Heroku ends any web request after 30 s and shows its "Application Error" page. The server keeps running: on 9/17 the bulk re-curates **did finish** (102 ok / 176, then **707 ok / 3,110 selected**). The staff just never saw the result. Bulk approve/ship of 100+ rows hits the same limit. | `activity_log` "Bulk recurate: …" 2026-09-17 09:57 / 10:27 |
| F4 | Thread 26: after bulk ship, the page shows every shipped box for the month, not the batch | **New feature, already quoted** as **A2 "approved batch export" (8 h / $160)** in the daily-orders thread. **NOT in this plan.** It's built only if the client accepts it on the new contract. | Slack quote A2 |
| F5 | Thread 20: manual Ship-To override | Not in SOW (SOW Phase 4 = single-row **kit** override, see `CHANGE_LOG.md`). **Quoted (6 h / $120), goes on the new contract with A2 and B. NOT in this plan.** | CHANGE_LOG.md |
| F6 | (found while checking) 335 pending decisions have **no kit** | Not in scope here. List it for Hasan and investigate after Oct 1. | `decisions` status=pending, kit_sku null |

---

## Task 1: Record the September manual shipments (P0, before Oct 1)

**Owner:** Senior does 1a. Junior does 1b–1d. Hasan runs `--apply`.

**1a (senior): extract the shared shipment writer.** Move the body of `ship_decision` (app.py ~8734) that writes history into a helper, and make the route call it, so there is exactly one way to record a shipment:
```python
def record_shipment_for_decision(db, d: dict, ship_date: date, note: str) -> str | None:
    """Mark decision d shipped and write shipments + shipment_items (B8 duplicate guard included).
    Does NOT touch stock, VeraCore, Shopify or Cratejoy. Returns the shipment id."""
```
Behaviour must stay byte-for-byte the same for the route. The existing tests must pass unchanged.

**1b (junior): `scripts/record_manual_shipments.py`**
- Args: `--shopify-csv PATH` (export of Sheena's sheet: *File → Download → CSV*), `--cratejoy-xlsx PATH` (optional), `--ship-date YYYY-MM-DD` (**required**, Hasan provides it), `--apply`.
- Parse the Shopify sheet: a row whose `ORDER NUMBER` is empty and column A has text is a **section header** holding the kit code (`CQ41`). Order rows start with `#OBB-`. Keep `kit_code`, `ORDER NUMBER`, `ORDER_EMAIL`, `SHIPMENT ID` (this is the Shopify **order id** = `decisions.order_id`).
- Kit code → SKU: `re.fullmatch(r"([A-Z]{2})(\d{2})", code)` → `f"OBB-{a}-{b} KITS"`. Look it up in `kits`; if it's missing, **stop the whole run** with an error.
- Match each row:
  1. `decisions` where `order_id == SHIPMENT ID` and status in (`pending`, `approved`).
  2. If there are none: the customer by email (`ilike`) + a decision in status `pending`/`approved` with `ship_date >= 2026-09-01`.
  3. 0 matches → `no_decision`. More than 1 (after step 1) → keep the newest by `created_at`, and the others become `duplicate` (see below).
- For each match:
  - If `kit_sku` differs from the sheet → set `kit_id`/`kit_sku` to the sheet's kit first (the kit actually sent).
  - Call `record_shipment_for_decision(db, d, ship_date, "Manual September shipment (Sheena sheet)")`.
  - Any other pending decisions for the **same order_id** → `status='rejected'`, `rejection_reason='Auto-rejected: duplicate of manual September shipment'`. The `Auto-rejected:` prefix is mandatory (so the kit isn't blacklisted).
  - A row whose customer already has a September shipment of that kit → `already`, write nothing.
- **Never:** decrement stock, push to VeraCore, fulfil in Shopify, touch Cratejoy.
- Print a summary and write `manual_shipments_report_<date>.csv` (order, email, decision id, kit, outcome).

**1c (junior): Cratejoy xlsx.** Hasan downloads *September CJ Renewals.xlsx* from Slack. Open it with `openpyxl`, print the headers, and **stop and show Hasan** before writing the mapping. The same matching rules apply, but match on `decisions.cratejoy_shipment_id` / subscription id instead of the Shopify order id.

**1d (junior): tests** in `tests/test_manual_shipments.py` (FakeDB, like `tests/test_upload_tracking.py`): a section header sets the kit; the kit gets swapped when different; a duplicate pending is auto-rejected with the prefix; a second run is a no-op (`already`); an unknown kit stops the run.

**Hasan:** dry run → read the report → `--apply` → spot-check 5 customers on their profile page.

---

## Task 2: Kit assignment respects stock (P0, before Oct 1)

**Owner:** Junior, reviewed by senior.

1. New pure helper next to `assign_kit`:
```python
def kit_reserved_counts(db) -> dict[str, int]:
    """kit_id -> number of PENDING decisions holding that kit (paginate 1000 at a time)."""
```
   Only `pending` counts. Approved boxes are already taken off `quantity_available` at approve time.
2. In `assign_kit`, right after `available_kits` is fetched (both the welcome and the regular query), filter:
   `free = k["quantity_available"] - reserved.get(k["id"], 0)`, keep `free > 0`, and log `sku, on_hand, reserved, free` for each kit dropped. If that leaves nothing, return the existing "No … kits with stock" result.
   Re-curate already rejects the old decision before calling `assign_kit` (`_recurate_customer_core` refuses when a pending decision exists), so the customer's own box is never counted. Add a test that proves it.
3. Apply the same `free` number in `override_kits_json` (app.py ~9173) so the override dropdown can't offer a fully reserved kit.
4. Kits page: add a **Reserved** column (from the same helper) next to Qty. This is display only.
5. Tests (`tests/test_kit_reservation.py`): 3 units, 5 customers → the first 3 get BT-21 and the 4th and 5th get the next suitable kit. A rejected decision frees its unit. The override list hides a kit whose units are all reserved.

**Known limit (write it in the PR):** between approve and the next VeraCore sync, a box is counted once (the approve decrement). That's correct. If VeraCore sync runs before the warehouse allocates the order, one unit can briefly look free. That's acceptable.

---

## Task 3: Bulk actions run in the background (P0 bug fix, before the first October bulk run)

**Owner:** Senior does 3a–3b. Junior does 3c–3d.
**Scope guard:** this fixes the "Application Error" only. It must **not** add a batch filter, a "show only this batch" view, or batch export. That's quoted item **A2** (new contract).

**3a. Migration `migrations/024_bulk_jobs.sql`** (one file):
```sql
create table if not exists bulk_jobs (
  id uuid primary key default gen_random_uuid(),
  action text not null, total int not null,
  done int not null default 0, succeeded int not null default 0,
  skipped int not null default 0, failed int not null default 0,
  status text not null default 'running',       -- running | done | error
  created_by text, created_at timestamptz default now(), finished_at timestamptz);
```
No change to `decisions`.

**3b. `bulk_decision_action`:** keep the validation and the in-flight claim exactly as they are. Then insert a `bulk_jobs` row, `background_tasks.add_task(_run_bulk_action, job_id, action, claimed_ids, ship_to_ob, user_email)`, and **return immediately** by redirecting to the same page as today (`redirect_qs`) plus `&job=<job_id>`. `_run_bulk_action` is the existing per-row loop moved **verbatim**, plus two additions:
   - Every 10 rows: update `done/succeeded/skipped/failed` on the job.
   - A `finally` block that sets `status`/`finished_at`, writes the same `log_activity` line as today, and **releases the in-flight claim** (today that's in the route's `finally`; move it).

**3c. Progress banner (junior):** when `?job=<id>` is on the Decisions URL, show one banner above the table: "Bulk recurate running: 57 of 120 done (3 skipped, 0 failed)". While running, add `<meta http-equiv="refresh" content="5">`. When done: "Bulk recurate finished: 117 done, 3 skipped, 0 failed." If there's been no progress for 10 min: "This run stopped (server restart). Select the remaining rows and run it again. That's safe: rows already done are skipped." Nothing else on the page changes. Filters work exactly as today.

**3d. Tests:** the route returns 303 immediately without running the loop (monkeypatch `add_task`). The job counters add up. The claim is released even if a row raises. A second submit of the same rows while running gets the existing "already running" message.

---

## Quoted, NOT in this plan (new contract, only if the client accepts)
| Item | Hours | Price |
|---|---|---|
| Manual Ship-To override (Thread 20) | 6 | $120 |
| A1 Today filter, or A2 approved batch export (Thread 26 / daily orders) | 3 / 8 | $60 / $160 |
| B Exception reporting and alerting | 17 | $340 |

---

## Order of work

| When | What |
|---|---|
| Today | 1a, 2 (branch `fix/kit-stock-reservation`), 1b–1d (branch `feat/record-manual-shipments`) |
| Before Oct 1 00:00 PT | Hasan merges Task 2, runs Task 1 dry → apply |
| Oct 1–2 | Task 3 (branch `fix/bulk-actions-background`), migration 024 run by Hasan first |
| After | F6 investigation (335 pending with no kit) |

## Definition of done
- [ ] 288 (+ Cratejoy) September boxes show as shipped, with history on each profile; no stray pending duplicates
- [ ] A new pending box never takes a kit whose free units are 0 (verified on the Oct 1 run: reserved ≤ on hand for every kit)
- [ ] Bulk re-curate / approve / ship of 300+ rows returns instantly, the banner shows progress, and no "Application Error"
- [ ] **Reminder: post the staff handover message** (bulk actions now show a progress banner, and there's a Reserved column on Kits)
