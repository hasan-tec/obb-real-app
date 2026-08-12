# Monthly Curation Report — Rebuild Plan

**Status:** draft v2 (audited against the Loom transcript, corrections applied)
**Sources:** Sheena's Loom walkthrough (Aug 2026) = primary. Aug 3 2026 call transcript =
secondary. DB verification = where stated. Every claim below is tagged with its source; anything
not observed is in §5 as an open question, not guessed.

---

## 1. Why we're rebuilding

The engine's Aug 2026 report showed **613** renewal customers against Sheena's manual **407**
(T1=12, T2=139, T3=142, T4=114).

Root cause: the pool is built from **who received a shipment in the last N months** (the
"Active Since" recency filter) — historical data. Tony's point on the call was that curation must
follow **who requires a shipment right now**, since the list changes daily.

Every recency window was tested. None reproduce her numbers:

| Window | Total |
|---|---|
| 1 month | 86 |
| 2 months | 171 |
| 3 months (current) | 604 |
| 4 months | 674 |
| 6 months | 786 |
| **Sheena actual** | **407** |

The filter cannot be tuned into correctness; it has to go.

> Note on 613 vs 604: 613 is the figure in the report generated 2026-08-01. 604 is the same logic
> recomputed later that day. Both are correct — the pool drifts as orders and cancellations land.

---

## 2. What Sheena actually does (Loom, with timestamps)

### 2.1 Building the customer pool
Copies the live order-data sheet, stamps it with the month, then **manually deletes cancellations**
because the sheet only adds new orders and renewals and never removes cancelled ones (13:30, 13:54,
16:21). Then filters by **due-date range** against the ship date to bucket trimesters (14:56–15:38).

September figures shown: **397 orders → 392 net** (14:14, 14:48). T2 = **69 − 1 = 68** (16:19, 16:33).

- The cancellation count itself is **inaudible** in the recording ("397 minus 2 … 3. 45."). The
  figure 5 is only implied by 397 − 392. Do not treat 5 as stated.
- The walkthrough is **Shopify only** — she says so twice (14:48, 16:41). **Cratejoy is never
  mentioned in the Loom.** That Cratejoy is processed separately comes from the Aug 3 call
  ("I process crate joy separately"), not from this video.
- **September has no T1 at all** — "no, we don't have T1" (15:32). Empty buckets are normal and
  the report must handle them.

Real quantities are only known **after renewals land on the 1st** (5:46). Before that she estimates
from the **previous month's build** — e.g. "from August we only built 130, that's just an estimate"
(5:46, 6:06). She builds only what is needed, no overbuilding (6:21).

### 2.2 Blocking is kit-recipe level, not a percentage
For September she lists every kit **built** in a given window and blocks **every item inside those
kits** (1:54, 5:14). No percentage or threshold appears anywhere in the process.

The kit→month mapping comes from a **separate "kit shipping schedule" sheet** covering 2022 to
present, listing "the SKUs that were **created** per month" (2:47–3:02). Her language is
consistently *built*, not *shipped* — "anything that was **built** in August CP 21" (4:02), "this
was **built** in May" (4:40).

She also builds each month's block list by **deleting from last month's list** rather than starting
fresh (3:28) — which is a cheap way for us to validate: this month's list should equal last
month's list shifted by one.

### 2.3 The window SHIFTS per trimester layer — this is the key mechanic

She blocks the current trimester's kits **and lower trimesters' kits**, because customers migrate
upward (2:18, 10:02). But each layer uses a **different month window**. Observed:

| Curating | Layer | Months used | Kits blocked | Timestamp |
|---|---|---|---|---|
| Sept **T2** | T2 | Jun, Jul, Aug | WKH2, CN21, CO21, CP21 | 3:47–4:40 |
| Sept **T2** | T1 | Jun, Jul, Aug | WKE1, BQ11 (BP11 deleted) | 4:57–5:25 |
| Sept **T3** | T3 | Jun, Jul, Aug | WKC3, CN31, CO31, CP31 | 11:06–11:34 |
| Sept **T3** | T2 carry | Jun, Jul, Aug | WKH2, CN21, CO21, CP21 | 11:45–11:57 |
| Sept **T3** | T1 carry | **Mar, Apr, May** | WKE1, BQ11, **BP11** | **12:21–12:39** |
| Sept **T4** | — | *not demonstrated* | *unknown* | — |

**This is why BP11 is deleted for T2 but kept for T3.** Any uniform "last 3 months" rule is wrong
and would produce the wrong block list.

Plausible reason (to confirm, not assume): a T3 customer was in T1 roughly six months ago, so their
T1 exposure sits further back than a T2 customer's. She says T3 and T4 follow "the same principle"
(6:40) but never demonstrates T4.

### 2.4 Other rules
- **Welcome kits are included** in block lists — WKH2, WKE1, WKC3 (3:40, 5:14, 11:06).
- **8 products per kit** — she works "until I'm able to come up with 8 products" (9:08, 12:50).
  Reads as a fixed stopping point. Confirmed in DB: all 12 July `CO-*` kits have exactly 8 items.
- **Composition:** T2 minimal baby items, T3 can take one or two, T4 a good mix (13:08).
  Ting specified CQ21 (T2) be mom-specific — **but low stock relaxes this**: "since we're very low
  in quantities, there may be slight adjustments" (7:38). A hard mom-only filter would not match
  her behaviour.
- She builds **one kit SKU per trimester per month** (CQ21 for Sept T2, 3:10).
- Availability is cross-checked against a **VeraCore product detail report** export (6:52–7:25).

---

## 3. Changes

### 3.1 Pool — delete the recency filter

Her actual rule is simply *"every row on this month's order sheet, minus the ones I deleted because
they cancelled."* Start with the closest equivalent and add conditions only if the gap demands it.

Source: the `decisions` table — the engine's native equivalent of her sheet, since a row is created
whenever a Shopify order or Cratejoy renewal lands.

Minimum rule:
- decision created within the cycle month
- `status` NOT IN (`rejected`, `shipped`)
- `customers.due_date` IS NOT NULL
- de-duplicate by `customer_id`
- trimester **recomputed live** from `due_date` vs `ship_date` — never the frozen
  `decisions.trimester` snapshot

**IMPLEMENTED 2026-08-12** as `load_renewal_pool_from_decisions()` in `curation_report.py`, selected
by a new `run_monthly_report(pool_source=...)` argument. **Default is still `"shipments"`** — the old
behaviour is untouched until the reconciliation below closes. `load_renewal_pool()` is unchanged, so
`projection_engine.py` is unaffected.

Re-measured 2026-08-12 (ship=14): pool 465 = **453 renewal + 12 new**.
Renewal by trimester: **T1=6 T2=136 T3=179 T4=132**.

> The earlier "457 / T1=6 T2=140 T3=177 T4=134" figure was taken a few days earlier at ship=15 and
> did not split renewal from new. Both are correct for their moment — the pool grows daily as
> decisions land. Always state the ship date and the measurement date alongside a pool count.

### The 407 comparison is not apples-to-apples — and it flips the conclusion

Sheena's Loom is explicit that her list is **Shopify only** ("for Shopify, we, this is only for
Shopify", 14:48). Splitting our pool the same way:

| Pool | T1 | T2 | T3 | T4 | Total | vs Sheena 407 |
|---|---|---|---|---|---|---|
| Sheena (manual, Aug) | 12 | 139 | 142 | 114 | **407** | — |
| Decisions pool, all platforms | 6 | 136 | 179 | 132 | **453** | +46 |
| **Decisions pool, Shopify only (renewal)** | **5** | **108** | **143** | **119** | **375** | **−32** |
| Cratejoy only | 1 | 28 | 36 | 13 | 78 | — |
| Current recency filter (3mo) | 17 | 186 | 214 | 207 | **624** | +217 |

If 407 is Shopify-only, we are **under by 32, not over by 46** — and the overshoot narrative that
motivated this rebuild inverts. T3 lands within +1 and T4 within +5; essentially the whole gap is
T2 (−31) and T1 (−7).

**Do not tune anything against 407 until its provenance is confirmed** (§5.8). Either way the
decisions pool is dramatically closer than the recency filter's 624, which remains the finding that
justifies the change.

Do **not** pre-emptively add a `subscription_status` condition. `cancelled-prepaid` has no basis in
the transcript — she deletes cancellations outright. Add filters only if reconciliation against her
number requires them, and record why.

Removing the "Active Since" form field is a template change; scope it as a deliberate UI task or
defer it (CLAUDE.md: don't touch templates unless the task is specifically UI).

Side benefit: this fixes Tony's reactivated-customer edge case for free, since a reactivated
customer generates a decision on renewal. Today 1,772 customers marked active have no shipment in
90+ days and are invisible to the report.

### 3.2 Blocking — per-layer, table-driven

Replace the 25%-of-group calculation with a lookup driven by §2.3's table. **Do not generalise into
a formula** — we have two observed cases and no T4 case, so a formula would be guessing.

```
blocked_items(trimester T, cycle month M) =
    for each (layer_trimester, month_window) in WINDOW_TABLE[T]:
        union of kit_items for every kit of that trimester built in that window
```

`WINDOW_TABLE` starts with only what was observed (T2 and T3 rows above). T4 stays unimplemented
until Sheena confirms it. Everything not blocked is CAN USE. Binary — no thresholds, no risk tiers.

**Blocker:** this needs kit **build month**, which currently lives only in her kit shipping schedule
sheet. `shipments.ship_date` is a proxy that may or may not agree. See §4.

### 3.3 Keep unchanged
- **Per-customer kit assignment** (`evaluate_existing_kit_coverage`) — full history plus
  alternatives. Different question, still correct. Leave alone.
- Trimester formula — verified against 600 customers, 0 mismatches. (Though see §5.6: her
  due-date boundaries are entered by hand and were garbled in the recording, so confirm they match.)
- Build-quantity maths and warehouse minimum.

### 3.4 Transcript-derived fixes
- Change default ship day 14 → **15**. Weakly supported: "we'll *probably* be shipping on
  September 15th" (15:16), plus the Aug 3 call. Confirm before treating as a standing rule.
- Handle empty trimester buckets cleanly (Sept has no T1).
- Report Shopify and Cratejoy separately (from the Aug 3 call, not the Loom).

### 3.5 Engine hygiene — not from the transcript, judge separately
- Label expired items `EXPIRED` instead of `0 / N · 0.0% · HIGH`, which reads as a bug.
- Cap `covered` by physical kit stock (one kit with stock 4 was recommended to 53 customers).
  Note this sits oddly against "we're only building the number we need to ship" (6:21) — she builds
  to demand rather than allocating from a shelf. Valid engine fix, but not her workflow.

---

## 4. Prerequisites

1. **July import** — 445 renewals (358 Shopify + 87 Cratejoy) missing. The 133 July records the
   engine has are all welcome kits, so combined the month is complete. Blocked on 2 unresolved item
   SKUs Sheena is creating.
2. **Kit build-month source.** Either get the kit shipping schedule sheet, or prove
   `min(shipments.ship_date)` per kit reproduces her build months. Until one holds, §3.2 cannot ship.
3. **Validate** the block list against her September lists in §2.3 before shipping.

---

## 5. Open questions

1. **T4 window table row** — T4 was never demonstrated. Which layers, and which month window each?
2. **Per-layer offsets** — confirm the T1-layer shift (Jun/Jul/Aug for T2 vs Mar/Apr/May for T3) is
   the intended rule and not a one-off. Is there a general principle, or is it judgement per month?
3. **`CO31 CN31`** — at 11:34 she says "CO31 CN 31, so the, I will not include", which is garbled and
   reads opposite to the parallel T2 case. Confirm they are blocked.
4. **New/first-time customers** — do they appear in T1–T4 counts, or stay on the welcome-kit track?
5. ~~**Kit build month**~~ **RESOLVED §7b.1** — the "OBB Kit Shipping Schedule" sheet holds it
   explicitly, one column per month per trimester. We need read access to that sheet, not a proxy.
6. ~~**Due-date trimester boundaries**~~ **RESOLVED §7b.4** — they reproduce the engine's formula to
   the day for a Sept 15 ship date. No change needed.
7. **Confirm (not open, just verify): 8 products is a hard stop, not a target.**
8. **Is the Aug 407 figure Shopify-only, or Shopify + Cratejoy combined?** This decides whether the
   engine is over- or under-counting, and therefore what we fix next. The Loom is Shopify-only for
   September; the provenance of the Aug 407 is not established. **Highest-value question on this
   list** — everything in §3.1's reconciliation depends on the answer.
9. If 407 is Shopify-only: what accounts for our **T2 shortfall of 31**? T3/T4 match closely, so a
   uniform cause is unlikely.

---

## 6. Non-goals
- Automating item *selection* (the mama/baby mix judgement). Surface the safe pool; a human picks.
- The **attribution sheet** — not needed; `decisions` is the engine-side equivalent.
  **The kit shipping schedule sheet is a different file and we probably do need it** (§4.2).
- VeraCore sync, kit composition entry, or anything else in Phase 3.

---

## 7. Sequence
1. Sheena creates the 2 missing July SKUs
2. Import July (445 renewals)
3. Resolve kit build-month source (§4.2)
4. Get T4 window row + confirm §5.2, §5.3
5. ~~Rebuild the pool (§3.1)~~ **DONE 2026-08-12** — reconcile against 407 once §5.8 is answered
6. Rebuild blocking (§3.2), validate against her September lists
7. Apply §3.4 / §3.5 fixes
8. Re-run Aug 2026 end to end

---

## 7b. Video frame evidence (2026-08-12) — two blockers resolved

Frames extracted from `Demo - Curation Process (1).mp4` (17:48, 1920×1200). These are *observations
from the recording*, not inference.

### 7b.1 The kit shipping schedule sheet EXISTS and its structure is now known (§4.2, §5.5 RESOLVED)

At 2:47–3:10 the tab is **"OBB Kit Shipping Schedule"**, a Google Sheet with year tabs
(2026, 2025, 2024, 2023, 2022, 2021) plus `Kit Inventory`, `Kit Expiration '24`, `Old Kit Inventory`.

Layout of the 2026 tab — four trimester blocks, each with a **Welcome Kit** row and a **Renewals**
row, one column per month:

| Block | Row | JUN | JUL | AUG | SEPT |
|---|---|---|---|---|---|
| T2 | Welcome Kit | WKH2 | WKH2 | WKH2 | WKH2 |
| T2 | Renewals | …21 (130) | CO21 | CP21 | *(blank)* |
| T3 | Welcome Kit | WKC3 | WKC3 | WKC3 | WKC3 |
| T3 | Renewals | …31 (150) | CO31 | CP31 | *(blank)* |
| T4 | Welcome Kit | AP41 | AP41 | AP41 | AP41 |
| T4 | Renewals | …41 (125) | CO41 | CP41 | *(blank)* |

**This is exactly the kit→month mapping §3.2 was blocked on.** It also carries the **build quantity**
in parentheses — `(130)` for the T2 June kit, matching "from August we only built 130" (5:46).

### 7b.2 T1 IS ON A DIFFERENT TIME AXIS — this explains the per-layer window shift

The T1 block's column headers are **not** month names. They are 15th-to-14th windows:

```
T1  :  … – JUL 14th | JUL 15th – AUG 14th | AUG 15th – SEPT 14th | SEPT 15th – OCT 14th | …
T2/3/4: JUN | JUL | AUG | SEPT | OCT | NOV | DEC
```

T1 Welcome Kit = WKE1 across those windows; T1 Renewals = BQ11.

§2.3 recorded that the T1 layer used Jun/Jul/Aug when curating T2 but **Mar/Apr/May** when curating
T3, and flagged it as unexplained. The two blocks being indexed on different axes is a concrete
mechanism for that difference. **This is a lead, not a proof** — it does not by itself derive the
three-month offset, so §5.2 stays open until Sheena confirms.

### 7b.3 Her curation workbook lays DO-NOT-USE out as one column per blocked kit

At 12:44 the tab is **"Advance OBB Curation"**, with one sheet per month+kit:
`SEPT CQ21 · SEPT CQ31 · AUG CP21 · AUG CP31 · AUG CP41 · JULY CO21 · JULY CO31 · JULY CO41 · …`

On the `SEPT CQ31` sheet: cell A2 is `CQ31 - 190` (kit being built + build qty 190, matching "Let's
see, 190" at 10:50). Columns B/C are UNIT PRICE / MSRP. Then **each subsequent column is headed by a
blocked kit SKU and contains that kit's item list** — column D = `WKC3` with 8 items, column E =
`CP31`. She builds the new kit in column A by cross-checking against those columns.

So DO-NOT-USE is literally "the union of the item lists of the blocked kits", laid out side by side.
That is the model in §3.2 — confirmed visually, not inferred.

### 7b.4 The trimester boundaries match the engine EXACTLY (§5.6 RESOLVED)

At 15:16 she states the September ship date is the 15th, then types the due-date ranges. The
transcript garbled them ("until 10:05", "0104270104"). Running `calculate_trimester`'s cutoffs for
`ship_date = 2026-09-15`:

| | Engine | Sheena (video) |
|---|---|---|
| T4 | due ≤ **2026-10-04** | — |
| T3 | **2026-10-05** … 2027-01-03 | *"for 3 until 10:05"* → **10/05** |
| T2 | **2027-01-04** … 2027-04-11 | *"T2 will be 0104 27"* → **01/04/27** |
| T1 | due > 2027-04-11 | *"no, we don't have T1"* — her data ends ~Feb 2027 |

Both boundaries reproduce to the day, and the absence of T1 is explained: no customer in her
September data has a due date beyond 2027-04-11. **The engine's trimester maths needs no change.**

---

## 8. Audit of the §3.1 implementation (2026-08-12)

An adversarial audit was run against `load_renewal_pool_from_decisions()`. The subagent was cut off
by a session limit partway through, so the remaining checks were completed directly against the live
DB with a read-only script that deliberately avoids importing the module (see the scheduler warning
below). Result: **no defects that change output.** Two cosmetic fixes applied.

**Verified correct:**
- **Backwards compatibility.** With `pool_source` at its default the old path is untouched. The two
  new `executive` keys are safe: `summary_json` is stored as JSON, `app.py` reads the dict with
  `.get()` defaults (`:5608`, `:5620`, `:8865`), and `templates/curation_report.html` accesses named
  keys only. `projection_engine.py` imports `load_renewal_pool`, which did not change.
- **Timestamp window.** `.gte("created_at", "2026-08-01").lt(..., "2026-09-01")` returns exactly the
  same 542 rows as a Python-side filter. All stored timestamps are `+00:00`, so there is no
  timezone-edge mis-bucketing. December rollover (`month == 12 → year+1`) is correct.
- **Numbers reproduce exactly:** 470 after status filter, 465 after dedupe, 453 renewal + 12 new,
  renewal T1=6 T2=136 T3=179 T4=132, Shopify renewal 375.
- **Platform fallback is unambiguous** — `customers.platform` holds only `shopify` (2,333) and
  `cratejoy` (154). No `both` rows exist, so the fallback cannot produce a mixed bucket.
- **No performance regression** — the new function pages 11,489 `shipments` rows to build the
  has-shipment set, which is what `load_renewal_pool()` already does (and it selects one column
  rather than two).

**Fixed after the audit:**
- The dedupe keeps the first decision per customer with no `ORDER BY`, so `decision_platform` would
  be arbitrary for a customer with two decisions on different platforms. Measured: 5 customers have
  >1 actionable decision this month, **0** differ on platform. Harmless today; documented in a code
  comment rather than adding speculative sorting.
- A log line asserted dropped decisions were "customer missing due_date" when the query cannot
  distinguish a NULL due_date from a missing customer row. Both are currently 0. Reworded to
  "no customer row with a due_date".

**Known drift:** the plan's Shopify-incl-new figure of 382 measured 383 a few hours later — one new
decision landed. This is the expected daily pool drift, not an error. Always pair a pool count with
its ship date and measurement time.

> **Scheduler hazard.** Importing `curation_report` transitively imports `app.py` (via the lazy
> import in `calc_trimester`), which starts the live background scheduler — it fired a Cratejoy
> daily sync during testing. Verified afterwards that it wrote **zero** rows to `decisions`,
> `customers`, `shipments` and `activity_log`, and that the newest `curation_runs` row is still the
> one from 2026-08-03. Prefer read-only scripts that re-implement the formula over importing the
> module.
