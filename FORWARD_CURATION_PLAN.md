# Forward Curation Plan — fix August decision labels, then make September work

**Status:** ready to implement, nothing done yet
**Written:** 2026-08-23
**For:** whoever picks this up next. Assumes no prior context on this project.

Two independent pieces of work. **Do Part 1 first** — it is small, it is a correctness fix on
live data, and Part 2 is easier to verify once the data reads correctly.

---

## Background you need before touching anything

Oh Baby Boxes ships a monthly subscription box. Each customer gets a "kit" (a box with ~8
items). The engine picks a kit per customer per month and records that choice as a row in
`decisions`. When a box physically goes out, that is a row in `shipments`.

**The single most important thing to understand:** `shipments` is the source of truth for what
a customer actually received. `decisions.kit_sku` records only *what the engine suggested at
the time*. Everything downstream — duplicate detection, DO-NOT-USE blocking, kit history —
reads `shipments`, never `decisions.kit_sku`. Verify this yourself before changing anything:

```
grep -n "received_kit_skus\|received_item_ids" app.py
```

Both are built from `shipments` / `shipment_items` inside `assign_kit()`.

In August 2026 the engine had problems, so Sheena (operations) processed the month by hand and
sent us a spreadsheet of what she actually shipped. That sheet was imported on 2026-08-22 by
`scripts/import_august_manual_assignments.py`. A follow-up script,
`scripts/finalize_august_cycle.py`, then cleaned up leftover `pending` decisions. Part 1 fixes
a mistake in that follow-up.

---

# PART 1 — 303 decisions are labelled "shipped" with the wrong kit

## What is wrong

`finalize_august_cycle.py` Phase B marked each affected customer's newest surviving `pending`
decision as `shipped`. It never checked whether that decision's kit matched what was actually
shipped. It usually did not, because the engine had suggested one kit and Sheena manually sent
a different one.

Worked example — customer `ctdjneal@gmail.com`:

| | date | kit | status |
|---|---|---|---|
| decision | 2026-08-01 | `OBB-BC-21 KITS` | **shipped** ← claims BC-21 went out |
| shipment | 2026-08-22 | `OBB-CP-21 KITS` | ← CP-21 actually went out |

The decision row asserts something that never happened. Sheena reads this page daily.

## Scale

Confirm the numbers before changing anything (they may drift slightly):

```python
# decisions the bad script touched, identified by its tight updated_at burst
touched = [d for d in decisions if (d.get("updated_at") or "")[:13] == "2026-08-22T21"]
# expected: 1446 total -> 1142 rejected (correct, leave alone), 304 shipped (the problem)
```

Of the 304 marked `shipped`: **303 have a `kit_sku` matching no August shipment for that
customer, 1 does match.** 39 of the 303 are June/July decisions, not even August — those
customers already had their August decision closed by an earlier bulk ship, so the script found
an older pending row and marked that instead.

## What is NOT wrong — do not "fix" these

- **No shipments were created or duplicated.** Phase B only ran `UPDATE` on decision status.
  August shipment counts reconcile exactly against the sheet. Leave `shipments` alone.
- **The 1,142 marked `rejected` (Phase A) are correct.** That was legitimate de-duplication of
  pending decisions stacked up since March. Do not revert those.
- **Curation logic is unaffected**, because it reads `shipments`. This is a display problem.

## The fix

Change the 303 mismatched rows from `shipped` to `rejected`.

**Why `rejected`:** what happened is the engine's suggestion was superseded by a manual shipment
of a different kit. The suggestion was never fulfilled. `rejected` is the only available status
meaning "closed, not fulfilled" — allowed values are `pending`, `approved`, `shipped`,
`rejected`.

**Why not rewrite `kit_sku` to the kit that shipped:** that falsifies history. The engine
genuinely did suggest BC-21. Keep the record honest; the shipment row already says what shipped.

**Safety check you must confirm yourself before running.** Marking decisions `rejected` feeds
`get_rejected_kit_map()` in `app.py`, which excludes recently-rejected kits from future
auto-suggestions. Read that function. It only counts rejections created *after* the customer's
most recent shipment:

```python
if last_shipment_at:
    query = query.gt("created_at", last_shipment_at)
```

Every affected customer now has a 2026-08-22 shipment and all these decisions predate it, so
they are filtered out and will not block any future kit. Confirm this holds for a sample of 5
customers before running the whole thing.

## How to implement

Write `scripts/fix_mislabelled_august_decisions.py`. Follow the convention in
`scripts/finalize_august_cycle.py`: dry-run by default, `--apply` to write, print the full plan
before touching anything.

Selection logic:

```
for each decision d where:
      d.status == "shipped"
  and d.updated_at starts with "2026-08-22T21"        # the burst the bad script created
  and d.kit_sku not in {kit_sku of that customer's shipments with ship_date in 2026-08}
->  set d.status = "rejected"
```

Do **not** widen the `updated_at` filter. Decisions Sheena marked shipped herself, through the
UI on 2026-08-22 between 15:58 and 16:06, are legitimate and must not be touched.

## Success criteria

1. Dry run reports **303** rows to change, none from the 15:58–16:06 window.
2. After apply, zero decisions exist where `status == "shipped"` and `updated_at` is in the
   21:xx burst and the kit matches no August shipment.
3. August shipment count is **unchanged** — measure before and after. If shipments changed,
   something is wrong; stop and investigate.
4. Spot-check `ctdjneal@gmail.com` in the UI: the 2026-08-01 BC-21 decision reads `rejected`,
   and the CP-21 shipment is still in Shipment History.
5. `python -m pytest tests/test_engine.py -q` passes (6 tests).

---

# PART 2 — September shows zero renewals

## What is wrong

Sheena tried to curate September and the report showed 0 renewal customers.

One input, `report_month`, currently controls three separate things:

1. **who is in the pool** — customers with a live order in that month
2. **the ship date** — which decides each customer's trimester
3. **the blocking windows** — which kits are off-limits

For September there are no orders yet, so the pool is empty and everything downstream reads
zero. Confirm:

```python
# decisions created per month, actionable = status not in (rejected, shipped)
# 2026-08: 97 actionable      2026-09: 0 actionable
```

This is not a bug in the pool logic — the pool correctly answers "who needs a box *right now*".
It is the wrong question for curation, because **curation always happens a month ahead.**

Sheena's own description, from her Loom walkthrough transcript
(`monthly curation process transcription.txt`, timestamps so you can check):

> **5:46** — "that's just an estimate because the actual quantities would only be determined
> after the renewals, which is after the first day of the month"
>
> **6:06** — "for purposes of this month, I would just base it on the previous month's
> quantities"

She curates September using **August's** customer list. The engine must do the same.

## The design

Split "who is in the pool" from "what we are curating for".

| Concept | Value | Drives |
|---|---|---|
| `report_month` | e.g. `2026-09` | ship date, trimester recalculation, blocking windows |
| pool month | always `report_month` minus 1 | which customers are in the list |

**Fixed at one month back. Do not add a picker** — explicit product decision. The Forward
Planner (`projection_engine.py`) already handles 3–6 month horizons; this report is the
one-month-ahead curation view.

**Include already-processed customers.** For a forward month the pool must include customers
whose previous-month box already shipped, because they are subscribers and will renew. Without
this, August's own pool collapses from 526 to 86 (most closed out by Part 1's script) and a
September pool built from it would be nearly empty. Add a UI toggle, defaulting **on**.

**Trimesters are recomputed** for every pooled customer at the report month's ship date. That is
the entire point of forward curation — the same people land in different trimesters as the ship
date moves:

| Ship date | T1 | T2 | T3 | T4 | total |
|---|---|---|---|---|---|
| 2026-08-14 | 29 | 180 | 180 | 137 | 526 |
| 2026-09-14 | 4 | 136 | 199 | 187 | 526 |
| 2026-10-14 | 0 | 91 | 184 | 251 | 526 |

**Blocking windows stay tied to `report_month`.** Do not change them. The rule is documented in
`CURATION_REBUILD_PLAN.md` §12 and is already correct and audited.

## Files to change

**`curation_report.py`**

- `load_renewal_pool_from_decisions(db, ship_date, report_month)` — add `pool_month: str` (which
  month's orders to read) and `include_processed: bool` (when True, skip the
  `status not in (rejected, shipped)` filter and take the whole cycle). Keep the
  `subscription_status in (active, cancelled-prepaid)` filter in both modes — that is how
  cancellations are excluded and it must not change.
- `run_monthly_report(...)` — compute `pool_month = report_month - 1`, pass it through, add
  `include_processed` defaulting to `True`. Put the pool month and pool size into the returned
  `executive` dict so the UI can display them.

**`app.py`**

- `generate_curation_report` route — accept the toggle from the form and pass it through.

**`templates/curation_report.html`**

- Add the toggle beside the existing form fields.
- In the report header, state the pool explicitly, e.g.
  *"Pool: 526 August orders → projected to September 14 ship date"*. Do not make the user infer
  where the number came from — that absence is what caused the original confusion.

## Watch out for

- **`projection_engine.py` must keep working unchanged.** It calls `generate_item_risk_report()`
  without `cycle_month` and relies on the legacy percentage path. Any new parameter needs a
  default preserving current behaviour; verify the Forward Planner still renders.
- **The scheduler** (`_monthly_report_scheduler` in `app.py`) calls `run_monthly_report` with no
  pool arguments. Make sure your defaults do something sensible there.
- **Do not "fix" the trimester formula.** It is verified against Sheena's own written boundaries
  to the day. See `CURATION_REBUILD_PLAN.md` §7b.4.

## Success criteria

1. A report for `2026-09` returns a pool of roughly **526** customers, split about
   **T1=4, T2=136, T3=199, T4=187** at a September 14 ship date. Small drift from new orders is
   expected; a difference of hundreds is not.
2. The report header names the pool month and count.
3. `2026-08` with the toggle **on** shows ~526; **off** shows ~86. Both are correct answers to
   different questions and both must be reachable.
4. September blocking is unchanged from `CURATION_REBUILD_PLAN.md` §12.5 — T2 blocks exactly
   `WKH2, CP21, CO21(+22/23/24), CN21, WKE1, BQ11` and **not** `BP11`.
5. Forward Planner still loads and produces the same numbers as before.
6. `python -m pytest tests/test_engine.py -q` passes.

## One assumption worth stating to the client

A forward pool is an **estimate**. It assumes August's customers renew in September. Real
September cancellations and genuinely new September orders are not knowable yet. This is the
same caveat Sheena already applies by hand ("that's just an estimate"), but it should be said
out loud rather than presented as a firm number.
