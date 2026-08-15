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

**WRITTEN 2026-08-12, but NOT WIRED as of the 2026-08-13 audit (§9).** `load_renewal_pool_from_decisions()`
exists in `curation_report.py`, selected by `run_monthly_report(pool_source=...)`. Default is
`"shipments"`, and **nothing anywhere passes `pool_source="decisions"`** — not the form
(`app.py:5304-5314`), not the job worker (`app.py:5366-5374`), not the scheduler (`app.py:1232-1240`).
It is currently unreachable from the live app. `load_renewal_pool()` is unchanged, so
`projection_engine.py` is unaffected — that part of the claim held.

**Two real defects found before this can be wired on, see §9 for full detail:**
- **F1 (silent empty report):** the pool filters `decisions.created_at` to *within* the report month.
  The UI defaults the report-month field to *next* month (`app.py:5262-5268`), which has no decisions
  yet. Selecting `pool_source="decisions"` today would render a 0-customer, 0-build report with no
  error and no warning — silently wrong, not a crash.
- **F2 (no status filter):** unlike `load_renewal_pool()`, the new function has no
  `subscription_status` filter. Measured live: 5 `cancelled-expired` customers are currently in the
  pool — exactly the group Sheena deletes by hand.

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

### 3.2 Blocking — SUPERSEDED BY §10. Read §10 first; this section is kept for history.

> **§10 (2026-08-13) replaces the design below.** The "we need her Kit Shipping Schedule sheet"
> blocker was wrong — kit→build-month is derivable from our own DB with 100% consistency. The
> `WINDOW_TABLE` "do not generalise" stance was also over-cautious: the structure IS regular, and
> three of the four §5 open questions are answered by the video. Details in §10.

### 3.2 (historical) Blocking — per-layer, table-driven

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

## 4. Prerequisites — §4.2 RESOLVED, see §10.1

> **Status as of 2026-08-13:** §4.1 (July import) is **done**. §4.2 ("get the kit shipping schedule
> sheet, or prove `min(shipments.ship_date)` reproduces her build months") is **resolved and was
> never actually a blocker** — kit build-month is derivable from our own DB, though via the *modal*
> ship month, not `min()`, which the original wording got wrong. See §10.1. §4.3 (validate the block
> list) is now spec'd concretely in §10.10.

1. **July import** — 445 renewals (358 Shopify + 87 Cratejoy) missing. The 133 July records the
   engine has are all welcome kits, so combined the month is complete. Blocked on 2 unresolved item
   SKUs Sheena is creating.
2. **Kit build-month source.** Either get the kit shipping schedule sheet, or prove
   `min(shipments.ship_date)` per kit reproduces her build months. Until one holds, §3.2 cannot ship.
3. **Validate** the block list against her September lists in §2.3 before shipping.

---

## 5. Open questions — MOSTLY RESOLVED, see §10

> **Status as of 2026-08-13 (§10):** 5.1, 5.3, 5.5, 5.6, 5.7 and 5.8 are all **answered** from the
> video/DB. 5.2 is the only one still genuinely open, and it has a safe default (§10.7). 5.4 and 5.9
> are reconciliation tasks, not blockers. The list below is kept for history — read §10 for current
> status.

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
5. Rebuild the pool (§3.1) — **function written, NOT wired, has 2 unfixed defects (F1/F2, §9)**
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

### 7b.3b Second video pass (2026-08-13) — stronger evidence than the transcript alone

Extracted 15 additional frames from the pool-building section (13:30–17:33) at Hasan's request, to
verify against the actual screen rather than the garbled audio transcript alone.

- **The order-data sheet's cancellation marking, seen directly:** rows for cancelled orders (e.g.
  "Alyssa Kr9m" #OBB-14224, "Chloe White" #OBB-14211) are shown with **strikethrough text + a
  pink/red row highlight**, not yet removed from the sheet. This is a flagging step that precedes
  actual deletion, refining §2.1's "manually deletes cancellations" — the mechanism is mark-then-delete,
  not delete-in-place.
- **Her own written trimester formula, found on screen (not spoken) — a stronger source than the
  audio transcript:** a Notepad window titled "TRIMESTER CALCULATION FORMULA" reads verbatim:
  `SHIP DATE + 19 DAYS = TRIMESTER 4 / + 13 WEEKS = TRIMESTER 3 / + 14 WEEKS = TRIMESTER 2 / + 13
  WEEKS = TRIMESTER 1`, followed by worked examples for three different ship dates (08/11 daily,
  08/15 "FINAL AUGUST", 09/15 "TEMP SEPTEMBER"). For 09/15/2026 her worked ranges are: T4 ≤ 10/04/2026,
  T3 10/05/2026–01/03/2027, T2 01/04/2027–04/11/2027, T1 04/12/2027–01/01/2029.
  **Independently re-computed the engine's own cutoffs for the same ship date: T4≤2026-10-04,
  T3 end 2027-01-03, T2 end 2027-04-11 — matches her written ranges exactly, to the day, on every
  boundary.** The "+13 weeks = T1" line and her 01/01/2029 upper bound do not correspond to anything in
  our engine, but the worked date (2027-04-11 + 13wk ≈ 2027-07-11) doesn't match her stated 01/01/2029
  either — that value reads as an arbitrary far-future placeholder her spreadsheet's range formula needs
  as a closing bound, not a real fourth cutoff. Our engine's T1 = "everything past T2" (open-ended) is
  consistent with this — no change indicated. She recomputes fresh cutoffs per ship date (three
  different sets for three different dates in the same note), matching our engine's design of taking
  `ship_date` as a parameter rather than using a fixed global boundary.
- **Live sheet counts, seen directly, corroborating the transcript's numbers:** the "TEMP SEPT" tab
  shows **Count: 69** while viewing what becomes the T2 filter (matches "69 minus 1... 68 T2" at
  16:19–16:33) and **Count: 156** on a later view consistent with the garbled T3 figure at 17:19-17:22.
- **Column layout confirmed on screen:** `ORDERS-DATA-V3` has `ALL ORDER COUNT, Q1, Q2, Q3, Q4, Q5`
  with **Q5 (due date) highlighted yellow** — matches the CSV export column layout `import_history.py`
  already parses (`ALL ORDER COUNT` at index 15, `Q5`/due date at index 20).

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

---

## 9. Wide-scope audit (2026-08-13) — every claim re-verified, real defects found

Requested explicitly because a prior narrower audit missed things. This one read every relevant file
in full (not grep excerpts) and re-derived every number live rather than trusting §8. Full findings
below; only the load-bearing ones are summarized here.

### 9.1 §3.1 is not actually shipped — it's unreachable

Confirmed by reading every call site: `app.py:5304-5314` (form), `app.py:5366-5374` (job worker),
`app.py:1232-1240` (scheduler) — none passes `pool_source`. §7 step 5 is corrected above from "DONE"
to accurate. This is a ~5-line fix (thread the param through), not a design problem — but it means
the report running in production today, right now, still uses the old recency filter.

### 9.2 F1 — CRITICAL, must be fixed before wiring §3.1 on

The pool is scoped to decisions created **within the report month**. The report form defaults to
**next month**. Next month has no decisions yet. Selecting the new pool for the default form state
returns an empty pool — 0 customers, 0 build quantity, every trimester empty — **with no error and no
warning**. This isn't a hypothetical: it's what the default UI state produces today if `pool_source`
were wired on as-is. Needs a decision (report the *current* cycle instead of next month's? warn loudly
on an empty decisions-window? fall back to shipments with a banner?) before §3.1 can ship, not just an
implementation task.

### 9.3 F2 — HIGH, no subscription_status filter

`load_renewal_pool_from_decisions` has no `subscription_status` condition, unlike `load_renewal_pool`.
Measured live: pool currently includes 5 `cancelled-expired` customers — precisely the group Sheena
manually deletes from her sheet every month. The plan's original "don't pre-emptively filter" call
(§3.1 body) was reasonable as a starting assumption but its consequence was never measured until now.

### 9.4 F3/F4 — MEDIUM, cosmetic but real

- Stored run parameters (`recency_months`, `include_paused`) get written to `curation_runs` and
  rendered in the report header ("Active since: last 3 months") **even when the decisions pool ignored
  them entirely** — misleading if anyone reconciles a stored run against its own parameters.
- `renewal_by_platform` (the Shopify/Cratejoy split from §3.4) is computed
  (`curation_report.py:796-799`) but never rendered anywhere and never persisted per-customer, so the
  "report separately" requirement is technically computed and practically invisible.

### 9.5 Numbers — mostly confirmed, two important drifts explained

Exact re-derivation matched on the load-bearing figures: T1=6, T2=136, T3=179 all reproduced exactly;
Shopify-only renewal reproduced **375, byte-for-byte** including the per-trimester split (5/108/143/119).
T4 drifted +3 (132→135, all Cratejoy) and total decisions drifted from normal daily activity — expected,
not a bug.

**One finding changes §1's headline numbers:** the July import (444 shipment rows, written 2026-08-12)
moved the *current recency-filter* pool from 624 to **638** — further from Sheena's 407, not closer.
§1's full recency-window sweep table (86/171/604/674/786) is now stale; re-measured today it's
174/581/638/704/816. This doesn't change the rebuild's justification (decisions-pool is still far
closer to 407 than recency-filter is), but don't quote §1's old table as current.

### 9.6 Confirmed clean (checked, not assumed)

Both pool functions return a strict-superset-compatible field shape — no downstream KeyError risk.
Empty pool doesn't crash anything (traced every consumer). Template can't KeyError on missing keys
(Jinja default-undefined). `projection_engine.py` genuinely untouched. Both July-import scripts still
enforce the never-seed-kits/items rule, re-verified against current file content. No migration since
001 changed a constraint the plan or code relies on. The old 25%/60% blocking algorithm
(`RISK_HIGH_THRESHOLD=60.0`, `RISK_MEDIUM_THRESHOLD=25.0`) is still exactly what runs — confirmed no
partial WINDOW_TABLE implementation exists anywhere in the repo (zero grep hits repo-wide).

### 9.7 New, previously untracked: the 1,099 stale `shipment_items` rows are confirmed still open

Exact figure reproduced: 1,099 missing rows across 383 shipments (70 with literally zero items),
concentrated in 2026-04 (213) and 2026-05 (123) — **none in July, already fixed**. This corrupts
input to the *current* DO-NOT-USE calculation today (it undercounts what customers actually received),
and will corrupt the future kit-recipe blocking too. Nothing blocks fixing it — the pagination fix
already exists in both import scripts' kit-cache loaders and just needs lifting into a one-time
backfill, same pattern as the July repair.

### 9.9 IMPLEMENTED 2026-08-13 — F1, F2, and wiring, all verified

Worked through §9.8's "blocked on nothing" list, one item at a time, verifying after each:

1. **Live latent bug fixed:** `/decisions/bulk-action`'s `kit_items` query (`app.py`, the
   "Issue 9" pre-fetch) was unpaginated — a bulk-approve batch spanning >~125 distinct kits would
   silently truncate at 1000 rows. Paginated it, matching the pattern already correct elsewhere in
   the same file (`app.py:4844`).
   **Correction (2026-08-13 audit):** the claim that this explains the April/May concentration in
   §9.7 is FALSE — checked directly. May 2026 has zero bulk-approved shipments at all (`notes`
   contains "Bulk-approved"); April has 42 vs 7,095 import-created. The stale rows trace to the
   historical import scripts, not this route. Also: at current scale (200 kits, ≤12 items each,
   29 distinct kits across all 1,900 pending decisions) this code path could not plausibly have
   hit the 1000-row cap regardless — it would hit a different limit first (see item 7 below). The
   fix is still correct and worth keeping as a defensive measure; the causal story attached to it
   was retrofitted and is retracted.
2. **1,118 stale `shipment_items` rows backfilled** (fresh count, up from 1,099 a day earlier —
   normal drift). Verified 0 remaining after.
3. **F2 fixed:** added `subscription_status IN (active, cancelled-prepaid)` to the decisions-pool
   customer query, matching `load_renewal_pool()`'s existing default. Verified live: pool dropped
   from 459 to 454, 0 `cancelled-expired` rows remain.
4. **F1 resolved — Hasan's call, not guessed.** Presented three options; chosen: report month
   defaults to the **current** month, not next month. Rationale confirmed by Hasan directly: the
   pool is meant to accumulate live through the month as real orders land (his own example — 300
   decisions the night of the 1st, +100 more by the 3rd, cancellations excluded via the existing
   status filter) — exactly how `load_renewal_pool_from_decisions` already behaves. The fix was
   only ever about which month the UI shows by default. Forward Planner's own "next month" default
   left untouched — different feature, correctly forward-looking by design.
5. **`pool_source` wired on.** Default changed from `"shipments"` to `"decisions"` in
   `run_monthly_report()`'s signature — the scheduler and the job worker both omit the parameter,
   so this one-line change activates it everywhere without a template/form change.
6. **Full pipeline re-verified end-to-end**, not just the pool function, via a from-scratch
   re-implementation of every step (pool → trimester grouping → kit-with-stock lookup → kit_items
   map → full customer history → per-trimester coverage evaluation) run directly against live data,
   deliberately avoiding importing `app.py`/`curation_report.py` (scheduler hazard). Result for
   2026-08 (current default month): **452 renewal + 3 new = 455 pool; T1=6 T2=136 T3=176 T4=134;
   337 of 452 covered by existing kits, 115 need new curation.** No error, no missing field, no
   crash — the audit's "no KeyError risk" claim is now empirically confirmed, not just traced.

**Not done, deliberately deferred:** browser-level UI test of the actual `/curation-report` route
was attempted (`.claude/launch.json` created, local `uvicorn` server started) but blocked at login
— real Supabase Auth, no credentials available, and entering a password on the user's behalf is
outside what this assistant will do regardless of authorization. DB-level verification above is
the substitute; a live-server pass is still recommended before this is fully trusted in production.

### 9.11 Second audit (2026-08-13) — found and fixed a real regression before push

Requested explicitly: independently re-verify §9.9's implementation, given the plan document and
transcript/video files. Full files re-read, every number re-derived live, video frames independently
re-extracted (not trusted from the earlier pass). Verdict: SAFE WITH FIXES — one critical finding,
now fixed; one pre-existing bug found and flagged, not yet fixed; two overclaims in this document
corrected above and below.

**C1 — CRITICAL, FIXED:** flipping `pool_source`'s default globally reintroduced F1 on the
*automated* scheduler path, which §9.9's fix never covered. The scheduler fires once, on the 1st of
the month at the first hourly tick ≥6am UTC, then locks for the month. Measured live: only 0-2
decisions exist for the new month by then across three real months checked (2026-06/07/08) — most
land 07:00-18:00 UTC that same day. The unattended auto-run would have silently generated a
near-empty report every month, indefinitely, with no error. **Fixed:** the scheduler call now pins
`pool_source="shipments"` explicitly, so the known-reliable old pool keeps running unattended. The
interactive form path (`curation_report_page`, now defaulting to current month) still gets the new
decisions pool — a human generating a report on demand isn't hit by the day-1 timing problem the
same way. **Open follow-up, not resolved:** should the scheduler's trigger move later in the month
(most decisions are in by day 3) so it can eventually use the new pool too? Real operational
decision, not guessed here.

**C2 — found, NOT fixed, flagging for a decision:** a *different*, pre-existing bug in the same
route — `db.table("decisions").select(...).in_("id", decision_ids).execute()` (the bulk pre-fetch,
unrelated to the pagination fix above) has no batching and fails outright above ~600-700 ids
(PostgREST/Kong URL length limit — confirmed live: 600 ids ok, 700 ids `400`, 1900 ids client-side
`InvalidURL`). **There are 1,900 pending decisions right now**, `/decisions` has a "select all"
checkbox with no pagination on the page, so selecting all pending and bulk-approving is broken
*today*, independent of anything in this rebuild. Fails loudly (caught by the route's outer
exception handler, no data corruption) rather than silently. Not touched — out of this task's scope
and needs its own decision, flagging so it isn't lost.

**Corrections to this document's own overclaims, per the audit:**
- §9.9 item 1's causal claim about April/May is retracted — see the correction inline above.
- §7b.4/§7b.3b's "matches the engine exactly / RESOLVED" is too strong. Independently re-checked
  all three of Sheena's worked Notepad blocks, not just the 09/15 one already cited: the 08/11 and
  09/15 blocks match the engine's cutoffs to the day on every boundary; the middle 08/15 block is
  off by 2 days on its T2 boundary AND is internally self-contradictory (T3 ends 12/03, T2 starts
  12/06, leaving 12/04-12/05 in no bucket) — reads as a hand-typing slip in HER note, consistent
  with §5.6's original "entered by hand, may not match" caveat, not a formula disagreement. 2 of 3
  match exactly; the plan's blanket "every boundary" claim didn't account for the third. No code
  change indicated.
- Minor: the cancellation-row highlight in §7b.3b is described as "pink/red" — re-checked directly,
  it reads as light purple/lavender. Cosmetic only, doesn't affect the mark-then-delete finding.

**Everything else independently re-confirmed, not just re-stated:** all headline pool/coverage
numbers reproduced exactly (455 pool, T1-T4 split, 337/115 coverage split, Shopify-only 375);
`subscription_status` filter confirmed load-bearing (removing it live-tests back to including 5
`cancelled-expired` rows); `report_month` default confirmed genuinely date-relative, not hardcoded;
exactly 2 callers of `run_monthly_report` exist repo-wide, no third caller pinning the old pool
silently; the two default-month code blocks (curation report vs Forward Planner) confirmed fully
independent; no orphaned imports from today's edits; `.claude/launch.json` confirmed to hold no
credentials and sits outside the git repo entirely (the repo root is `opus-obb-prototype/`, not the
parent folder). Full transcript re-read; every timestamped claim checked held except one trivial
timestamp slip (15:38 not 15:32 for "we don't have T1") that doesn't affect any conclusion.

### 9.10 Second video pass evidence, folded into confidence in the above (2026-08-13)

Re-extracted 15 frames from the pool-building section (13:30–17:33) — see §7b.3b. Two findings
directly relevant to today's fixes:
- Her own written trimester formula (a Notepad reference, not just spoken audio) reproduces the
  engine's cutoffs to the day across all three boundaries for a 09/15/2026 ship date — stronger
  confirmation than the garbled transcript alone gave in §7b.4.
- Her own "TEMP SEPTEMBER" labeling directly informed the F1 decision above: she previews a future
  month's numbers *before* they're final, using partial data, exactly matching what the
  current-month default now does for us going forward.

### 9.8 Revised "what's left," superseding earlier turns' lists

**Blocked on Sheena (cannot proceed without her):** sheet access for kit build-month (§4.2), the T4
window row (§5.1), per-layer offset confirmation (§5.2), the garbled `CO31 CN31` line (§5.3), 8-products
hard-stop confirmation (§5.7), and §5.8 — still the highest-value open question.

**Blocked on nothing, just not built — updated 2026-08-13, items 1-3 & 8 now DONE (§9.9):**
1. ~~Wire `pool_source="decisions"` through~~ **DONE.**
2. ~~Resolve F1~~ **DONE** — current-month default, Hasan's explicit decision, §9.9.
3. ~~Add the `subscription_status` filter~~ **DONE.**
4. Render `renewal_by_platform` somewhere, or stop computing it (§9.4) — still open.
5. Ship day 14→15 (§3.4) — still open, pending confirmation it's a standing rule.
6. EXPIRED item labelling (§3.5) — still open.
7. Cap `covered` by kit stock in the monthly report (§3.5) — still open, already written for the
   Forward Planner (`projection_engine.py:366-376`), just needs porting across.
8. ~~The stale `shipment_items` backfill~~ **DONE** — plus the live recurring cause (unpaginated
   bulk-approve query) fixed, so it won't reoccur at the next large batch approve (§9.9 item 1).

---

# 10. THIRD VIDEO PASS (2026-08-13) — most "blockers" were not blockers

Hasan pushed back on the §9.8 blocker list: *"why do we need it cant we do it without it... see the
video bruh... alot can be answered in the video."* He was right on five of six. This section
supersedes §3.2 and rewrites §4/§5. Everything below is either an on-screen observation or a live DB
measurement — sources named per claim.

## 10.1 RESOLVED — kit build-month is derivable from OUR OWN DB. Her sheet is NOT needed.

This was §4.2, the "single hard blocker." It is not a blocker. Measured against `shipments`,
grouping `kit_sku` by ship month:

```
prefix -> peak ship month, all kits with >=10 shipments, 31 prefixes:
  CA=2025-05  CB=2025-06  CC=2025-07  CD=2025-08  CE=2025-09  CF=2025-10
  CG=2025-11  CH=2025-12  CI=2026-01  CJ=2026-02  CK=2026-03  CL=2026-04
  CM=2026-05  CN=2026-06  CO=2026-07
30 of 31 prefixes are CONSISTENT (every kit sharing a prefix — all trimesters, all size
variants — peaks in the SAME month). Concentration is 93-100%, usually 100%.
```

Two independent derivations, and they agree:

1. **Modal ship month per kit SKU.** Cross-checked against the three months visible in her Kit
   Shipping Schedule frame (§7b.1): she has T2/T3/T4 renewals as CN=JUN, CO=JUL, CP=AUG. Our modal
   month reproduces **CN→2026-06 and CO→2026-07 exactly, for all three trimesters** (6/6 kits).
   `min(ship_date)` does NOT work — a handful of early stragglers skew it (CN-21's first ship is
   2026-05-02 though 99% of its 154 shipments are in June). **Use the mode, not the min.**
2. **The SKU prefix itself encodes the month.** The second letter increments one per month:
   `CA(2025-05) → CB → CC → … → CO(2026-07)` — **15 consecutive months, zero breaks.** So even for
   a kit with no shipments yet, the month is arithmetic. (Pre-2025-04 there are legacy one-offs —
   BC/BD interleaved with BX/BY — so treat the letter rule as reliable from `CA` forward and fall
   back to modal ship month for older kits.)

**Consequence:** §4.2 is closed. We never needed read access to her sheet for this. Getting it would
still be a nice cross-check, but nothing is blocked on it.

**The one real gap this leaves:** the *current* month's kit has no shipments yet (CP-21/31/41 = August
= 0 shipments in our DB, because we've imported through July). That does not matter for blocking,
because the block window always looks at *past* months — by the time we curate September, August's
shipments exist. And the letter rule covers it regardless.

## 10.2 RESOLVED — "CO31 CN31" ARE blocked. The transcript was mis-transcribed.

§5.3. The transcript reads *"CO31 CN 31, so the, I will not include"* (11:34), which contradicted the
parallel T2 case. Extracted the frame at 11:40 — the `SEPT CQ31` sheet, scrolled right:

```
col D = WKC3   col E = CP31   col F = CO31/32/33/34   col G = CN31   col H = WKH2
```

Columns F and G are **fully populated with item lists** (BABYMAMATANK, ULTRASOUNDWOODENPHOTOFRAME,
GOLDENBRICKROAD…, and AVOCADOHEALTH, BABYINBELLY, BIRTHINGAFFIRMATIONCARDDECK…). They are in the
block list. The audio was almost certainly *"so the— I will now include"* or similar. **CO31 and CN31
are blocked. Question closed.**

## 10.3 RESOLVED — the block list's structure, seen directly on screen

Both curation sheets, read off the frames rather than inferred:

```
SEPT CQ21  (curating T2):  WKH2 | CP21 | CO21/22/23/24 | CN21 | WKE1 | BQ11 | BP11
SEPT CQ31  (curating T3):  WKC3 | CP31 | CO31/32/33/34 | CN31 | WKH2 | … (T2 block) … | WKE1 | BQ11 | BP11
```

The pattern is regular, not ad-hoc: **for each trimester layer from T down to T1, one column for that
layer's welcome kit, then one column per recent renewal kit of that layer.** Column A is the kit being
built; A2 carries the build quantity (`CQ21 - 130`, `CQ31 - 190`) — the previous month's build used as
an estimate, exactly as she describes at 5:46.

**Size variants are collapsed into one column** (`CO21/22/23/24`). But our DB shows the variants are
**not** identical — each of CO-22/23/24 differs from CO-21 by 2 items (same for the 3x and 4x
families). So when we implement this, block the **union of all size variants**, not just the `-x1`
variant. Her single column is a shorthand that happens to be safe because she eyeballs the whole
family; ours has to be explicit.

## 10.4 RESOLVED — T4 needs no separate investigation

§5.1 claimed T4 "can't ship without" a demonstration. Two pieces of evidence say otherwise:
- She states it directly at 6:40: *"it's also the same way when I build for T3 and T4, same
  principle."*
- Her workbook tab bar (visible in every curation frame) shows **`AUG CP41`** and **`JULY CO41`** —
  T4 sheets already exist and are built the same way. She simply didn't open one on camera.

The §10.3 structure generalises with no new information: curating T4 → `[T4 welcome + T4 renewals] +
[T3 layer] + [T2 layer] + [T1 layer]`. **Not a blocker.** If the generalisation is ever wrong, it will
show up in validation (§10.7), which is the right place to catch it.

## 10.5 RESOLVED — the 407 figure is Shopify-only

§5.8, previously called "the highest-value open question." She says it explicitly, twice:
*"So that's 392, so for Shopify, we, this is only for Shopify. So for Shopify this is 392."* (14:48)
and *"Yeah, for Shopify."* (16:41). Her monthly count is Shopify-only **by construction** — Cratejoy
is a separate pass (confirmed independently by the Aug 3 call: *"I process crate joy separately"*).

The number 407 itself is never spoken in the Loom, so this is an inference about her *process*, not a
direct quote about that specific figure — but the process is unambiguous, and the September figures
she does narrate (397 → 392) are Shopify-only.

**So the correct comparison is Shopify-only vs Shopify-only:** our decisions pool gives **375 Shopify
renewal** vs her 407 → we are **under by ~32**, not over. §3.1's "the overshoot narrative inverts"
note was right, and is now confirmed rather than conditional. The remaining ~32 gap is §5.9 and is a
real thing to chase, but it is a reconciliation task, not a blocker.

## 10.6 NOT a blocker — the 8-products rule

§5.7. Confirmed as a stop condition inside **her kit-curation** step (*"until I'm able to come up with
8 products"*, 9:08), which is explicitly a **non-goal** for us (§6 — a human picks the items). It
never enters the pool or the DO-NOT-USE calculation. Removed from the blocker list entirely.

## 10.7 RESOLVED IN §11 — the window EXTENDS. Section kept for history.

> **Superseded 2026-08-15 (§11.1).** This was called "the one genuine blocker" and it should not have
> been. The answer was in the Loom transcript the whole time, and the video confirms it visually.
> **The lower-trimester window extends back, it does not shift.** Read §11.

## 10.7 (historical) The per-layer month window

§5.2, and it is the only §5 item that survives. The evidence is genuinely mixed and I am not going to
resolve it by picking whichever reading is convenient:

- She narrates the T1 layer as **Jun/Jul/Aug** when curating T2 (4:57), and **Mar/Apr/May** when
  curating T3 (12:21, repeated three times).
- But the *sheets* for BOTH T2 and T3 contain the same T1 columns — `WKE1 | BQ11 | BP11`. At 5:25 she
  says of the T2 sheet *"it's safe to delete BP11"*, yet the frame 3 seconds later still shows BP11
  populated, and I could not obtain a clean scrolled-right frame after that edit to confirm whether
  she actually removed it.

So: does the lower layer's window **shift back** (Mar-May instead of Jun-Aug), or **extend back**
(Jun-Aug *plus* Mar-May)? The narration suggests shift; the column layout suggests extend.

**This does not block implementation, because the safe choice is knowable:** extending back blocks a
superset — it can only ever mark an item DO-NOT-USE that she might have allowed, never the reverse.
For a duplicate-avoidance system, a false "don't use" costs a little inventory flexibility; a false
"can use" ships a customer the same product twice. **Default to the wider window; flag the difference
in the report so a human can see which items were caught only by the wider rule.** Then validate
against her actual September lists (§10.8) and tighten if it's over-blocking.

Worth noting the mechanism §7b.2 found — T1 is tracked on a 15th-to-14th axis while T2/T3/T4 use
calendar months — is a plausible reason the T1 layer behaves differently from the others. Still a lead,
not a proof.

## 10.8 What §3.2's implementation actually looks like now

Replacing the old `WINDOW_TABLE` design. Hasan's own framing of it was correct: *"checking each
trimester and the kits items and then querying from all the items and if it was in that list she made
then its not included and if it isn't then can use list."*

```
build_month(kit)  =  modal ship month of that kit_sku in `shipments`
                     (fallback: 2nd SKU letter, CA=2025-05 +1/month — see 10.1)

blocked_items(curating trimester T, cycle month M):
    for layer L from T down to 1:
        window = the last 3 calendar months before M      # widen per 10.7 for L < T
        for every kit K where trimester(K) == L and build_month(K) in window:
            include ALL size variants of K                # 10.3 — variants differ by ~2 items
            add every item in kit_items[K]
    -> DO NOT USE = that union;  CAN USE = all other items
```

Binary, no thresholds, no risk tiers — which is what she actually does. The existing 25%/60%
percentage model (`RISK_HIGH_THRESHOLD` / `RISK_MEDIUM_THRESHOLD`, `curation_report.py:28-29`) has no
counterpart anywhere in her process and gets deleted, not tuned.

**Prerequisites remaining: none that are external.** This can be built now.

## 10.9 Rewritten blocker list — supersedes §9.8

**Genuinely blocked on Sheena: nothing.** Every §5 question except §5.2 is answered above, and §5.2
has a safe default (§10.7).

**Worth asking her anyway — as confirmation after the fact, not as a gate:**
- Does the lower-trimester window shift back or extend back (§10.7)? Cheapest possible confirmation
  of the one real unknown.
- Did BP11 stay in or come out of the September T2 list?

**Blocked on nothing, ready to build:**
1. §3.2 blocking rebuild, per §10.8 — the big one, now unblocked.
2. Scheduler: move the monthly trigger from day 1 → **day 3** and drop the `pool_source="shipments"`
   pin added in §9.11, so the auto-run uses the new pool. Hasan's decision, 2026-08-13. Day 3 is
   supported by the measurement in §9.11 (day-1 06:00 UTC sees 0-2 decisions; the bulk land 07:00-18:00
   on day 1, so day 3 is comfortably clear). **Code change queued, deliberately not made in this pass
   — this pass was plan-only.**
3. Render or drop `renewal_by_platform` (§9.4). Given §10.5, rendering it is now clearly the right
   call — the Shopify/Cratejoy split is exactly what reconciliation against her 407 needs.
4. Ship day 14 → 15 (§3.4).
5. EXPIRED item labelling (§3.5).
6. Cap `covered` by kit stock in the monthly report (§3.5) — already written in
   `projection_engine.py:366-376`.
7. C2 from §9.11 — bulk-approve breaks above ~600 selected decisions. Unrelated to this rebuild,
   still real, still unfixed.

## 10.10 Validation plan for §3.2, once built

We have her actual September answers on screen, so this is directly checkable:
- Curating **Sept T2**, the block list must equal: `WKH2, CP21, CO21(+22/23/24), CN21, WKE1, BQ11`
  (± BP11 per §10.7).
- Curating **Sept T3**: `WKC3, CP31, CO31(+32/33/34), CN31, WKH2, CP21, CO21, CN21, WKE1, BQ11, BP11`.
- Her cheap self-check applies to us too (§2.2): this month's list should equal last month's list
  shifted by one — so `AUG CP21`'s list should be September's shifted back a month. Two of her past
  months (`AUG CP21/CP31/CP41`, `JULY CO21/CO31/CO41`) are visible as tabs and could be requested as
  a second validation set if the September check is ambiguous.

---

# 11. FOURTH PASS (2026-08-15) — the "blocker" was answerable all along

Hasan asked whether the two remaining questions could have been answered from material already in
hand (Loom transcript, the video, and the Aug 3 meeting transcript). **Yes — both.** I was
over-cautious in §10.7 and asked Sheena a question the transcript already answers. Recording it
properly here so it doesn't get re-asked.

## 11.1 ~~RESOLVED — the window EXTENDS~~ **WRONG — CORRECTED IN §12. Do not use this section.**

> **This conclusion is incorrect.** §12 has the authoritative rule, taken from Sheena's own Word
> document (the artifact she writes the block list in, which I had never seen when I wrote this).
> The window **SHIFTS back 3 months per layer**; it does not extend. What follows is kept only so the
> error is traceable.

## 11.1 (WRONG, historical) The lower-trimester window EXTENDS back, it does not shift

§10.7 called this "the one genuine blocker" and framed it as shift-vs-extend. **It's extend**, and
there are three independent confirmations.

**1. The Loom transcript says it outright.** The lines I had already read, verbatim:

```
12:21  This one for WKE1 May, April and March.
12:25  So, May, April, and March, May, April, and March.
12:34  So that will be WKE one and then.
12:39  BQ 11 and then there's BP 11.
```

She names **all three** — WKE1, BQ11 *and* BP11 — for T3's T1 layer. I fixated on the "May, April and
March" window (which suggested a shift) and under-weighted the list she actually reads out one line
later. The window phrase describes where the *older* kit sits; the list is what actually gets blocked.

**2. The video confirms it visually.** Frame at 12:40 on the `SEPT CQ31` sheet, scrolled right:

```
col J = CO21/22/23/24   col K = CN21   col L = WKE1   col M = BQ11   col N = BP11
```

Column N's item list is **identical, in the same order, 8 of 8**, to the `BP11` column on the
`SEPT CQ21` sheet (SUNFLOWERMOTHERHOOD, HAPPYSHOPPE, WILLOWCOLLECTIVE, ATTITUDE+STRETCHMARK,
BIRTHKEEPINGBYBEKAH, PANTSEXTENDER, AZALA+COOLINGBODYBALM, NURIA+HYDRATE). So the T3 sheet carries
both T1 renewal kits.

**3. The contrast with the T2 sheet holds.** On `SEPT CQ21` she says *"it's safe to delete BP11"*
(5:25) — T2's T1 layer keeps only WKE1 + BQ11.

**So the rule is:**

| Curating | T1 layer contains | Span |
|---|---|---|
| T2 | WKE1, BQ11 | last ~3 months of T1 kits |
| T3 | WKE1, BQ11, **BP11** | last ~6 months of T1 kits |

Each trimester deeper reaches ~3 months further back, because a T3 customer has been subscribed
longer and has therefore seen more past kits. This makes §10.8's pseudocode concrete — the window for
layer L when curating trimester T is **the last `3 × (T − L + 1)` months**, not a fixed 3-month window
and not a shifted one.

**Consequence for §10.7's "default wide and flag it" hedge: no longer needed.** We know the rule.
Implement it directly.

## 11.2 The 407 figure — origin traced, and a contradiction worth knowing about

**Origin:** the Aug 3 meeting, not the Loom. Sheena reads the numbers out one at a time —
T2 = 139 (line 544), T3 = 142 (line 628), T4 = 114 (line 630), T1 = 12 — and Tony recaps them as
*"the 12 139 142 and 114"* (line 665). Sum = 407.

**How she builds them (meeting, ~line 668):** from the **attribution sheet**, *"a spreadsheet where
[Amayis] shared to us … whenever there are new customers from both from new orders and renewals would
populate"*. Tony restates it as *"an auto pool … for all orders that need to be essentially orders
that need to be shipped, including renewals as well as new orders"*, then filters by due date to split
trimesters. Sheena: *"Yes, correct."*

**The contradiction.** In the meeting, both Sheena and Tony describe that sheet as containing **new
orders AND renewals**. In the Loom she says of the order-data tab *"this is where the new orders would
enter daily"* (13:30). But in Slack (2026-08-13) she answered *"that sheet only have renewal
customers."*

Those cannot all be true of the same sheet. Most likely reading: the sheet **auto-populates with
both**, and she **filters to renewals** when producing the T1–T4 curation counts — which would make
her Slack answer right about the counts and the meeting right about the sheet. But that is a
reconciliation I am inferring, not something anyone stated.

**Why it matters:** it decides whether our Shopify-only renewal figure of 375 should be compared
against 407 directly (→ 32 customers genuinely missing from our data) or against something smaller
(→ smaller or no gap). Do **not** tune the pool against 407 until this is settled. This is now the
only genuinely open question in the whole plan.

**Also worth noting from the same meeting:** Tony's own articulation of why the recency filter is
wrong is sharper than anything in §1 — *"we can't curate based on what we've shipped in the past. We
have to curate based on which customers require a shipment, because customers cancel … that number
shifts monthly … our customer list is not static."* That is exactly the justification for the
decisions-based pool, from the client, unprompted.

## 11.3 Process note — check what we already have before asking

Both §10.7 and the first-timers question were answerable from files already on disk. The first-timers
one Sheena had *already* answered in Slack ("that sheet only have renewal customers") and it was still
carried in a follow-up draft. Cost: a round-trip with a client who is stretched thin, and a call
booked over something the transcript answers.

**Rule going forward:** before adding anything to a question list for Sheena, grep the Loom
transcript, the Aug 3 meeting transcript, and pull the relevant video frames first. Only what survives
all three gets asked.

## 11.4 Updated blocker list

**Genuinely open — exactly one:**
- §11.2. Does her 407 (12/139/142/114) include first-time customers or not? The meeting and her Slack
  reply disagree. Everything else about reconciliation waits on this.

**Answered, do not re-ask:**
- Per-layer window → §11.1, extends back, `3 × (T − L + 1)` months.
- `CO31`/`CN31` blocked → §10.2.
- T4 same principle → §10.4.
- Kit build-month derivable from our own DB → §10.1.
- 8-products stop condition, out of scope → §10.6.
- Trimester boundaries match to the day → §7b.4 / §7b.3b.

**Ready to build, blocked on nothing:** §3.2 blocking rebuild per §10.8 + **§12's** window rule (NOT
§11.1's — that was wrong), the scheduler day-3 move (§10.9 item 2), and the §9.8 list items 4–7.

---

# 12. THE AUTHORITATIVE BLOCKING RULE (2026-08-15) — from Sheena's Word document

Hasan supplied screenshots of the **Word document** Sheena builds the block list in — the artifact
referenced at Loom 1:19 (*"I typically would just use a pen and paper… but for this demonstration, I
would be doing it on Word"*). This is the explicit statement of the rule and it **corrects §11.1**,
which concluded the opposite.

## 12.1 What the document says, verbatim

```
SEPTEMBER CURATION

        T2                          T3
SEP  – T2                    SEP  – T3
AUG  – T2 – T1               AUG  – T3 – T2
JULY – T2 – T1               JULY – T3 – T2
JUNE – T2 – T1               JUNE – T3 – T2
MAY  -                       MAY  -        - T1
APR  -                       APR  -        - T1
MAR  -                       MAR  -        - T1
FEB  -                       FEB  -
JAN  –                       JAN  –
DEC  -                       DEC  -
```

Read it as: **"for month X, block the kits of trimester Y that were built in month X."** The months run
down the left; the trimester tags say which layer is in scope for that month.

## 12.1b CONFIRMED BY SHEENA IN WRITING (2026-08-15)

Asked directly — *"When you're picking items for a T3 box, do you avoid using anything from the last
three months of T1 kits, or anything from three months further back than that?"* — she replied:

> *"When curating a T3 box, we always look within the 3-month period that corresponds to T3. The
> reason is that pregnancy is divided into 3 trimesters, with each trimester covering roughly 3
> months… So the rule isn't simply 'avoid anything from the last 3 months of T1' or 'avoid anything
> older than 3 months.' It's more about identifying which 3-month trimester period the box belongs
> to, then curating based on that period.*
> *For example: T1 = months 1–3, T2 = months 4–6, T3 = months 7–9.*
> *The important thing for the system is that each trimester has its own 3-month window, and the
> products assigned to that trimester should be evaluated within that window."*

This independently confirms §12.2's shift model, from her own description of the mechanism: a customer
in T3 is at pregnancy months 7–9, so she was in T2 three months ago and T1 six months ago. Each layer
therefore gets **its own** 3-month window, offset by ~3 months per layer — not one window that grows.

**§11.1's "extend" reading is now doubly refuted** (Word doc + her written confirmation). The
spreadsheet columns that suggested it were captured mid-deletion.

## 12.2 The rule

**The window SHIFTS BACK 3 months per trimester layer below the one being curated. It does not extend.**

For curating trimester **T** in cycle month **M**, with depth `d = T − L` for each layer `L`:

| depth | layer | months blocked |
|---|---|---|
| 0 | T itself | M, M−1, M−2, M−3 |
| 1 | T−1 | M−1, M−2, M−3 |
| 2 | T−2 | M−4, M−5, M−6 |
| 3 | T−3 | M−7, M−8, M−9 |

Both the **welcome kit** and the **renewal kit** of each in-scope layer/month are blocked, and **all
size variants** of a renewal kit (they differ by ~2 items each — §10.3).

**Why it's shaped this way — it tracks the customer's own history.** A customer who is T3 this
September was T2 roughly three months ago (Jun–Aug) and T1 roughly six months ago (Mar–May). The rule
blocks precisely what that cohort could already have received. That is also why depth 0 reaches one
month further forward than depth 1: the current month's own kit for the trimester being curated is in
scope, but a lower layer's current-month kit is not, because this cohort has already moved past it.

## 12.3 Why §11.1 got it wrong, and what that means for the spreadsheet evidence

§11.1 concluded "extend" from the `SEPT CQ31` spreadsheet showing **both** `BQ11` and `BP11` columns
populated. Under §12.2's rule, T3's T1 layer is Mar/Apr/May only, so `BQ11` (a Jun–Aug kit) should
**not** be there.

Most likely explanation: she builds each month's sheet by **deleting from the previous month's sheet**
(3:28 — *"since I already built this one, so it's easier for me to just delete the SKUs that we no
longer be included"*), so the frame I captured was mid-deletion. The spoken line *"BQ 11 and then
there's BP 11"* (12:39) reads equally well as her scanning the schedule to locate the right kit.

**The Word document wins**: it is her explicit plan of which months map to which layers, written before
the spreadsheet work, and it is internally consistent across both the T2 and T3 pages. The spreadsheet
is a working artifact captured mid-edit.

**Validation this gives us for free:** if our implementation is right, `SEPT CQ31`'s T1 layer should
resolve to `WKE1 + BP11` and **not** `BQ11`. That is a direct, checkable prediction — see §12.5.

## 12.3b VERIFIED AGAINST LIVE DATA (2026-08-15) — three findings that change the implementation

A verification pass against the live DB proved the §12.4 sketch below is **wrong in three places**.
One rule does not fit all kit types. Corrected shape is in §12.4b.

### (a) `age_rank`, not modal ship month, is the build-month key

```
month(age_rank r) = 2024-03 + (r − 65) months     — exact, 1:1, 0 ambiguous ranks across 65→93
age_rank is assigned per SKU-prefix block:  CM=91  CN=92  CO=93  CP=94  →  CQ (Sept 2026) = 95
```

**Modal ship month is blind exactly where it is needed.** `OBB-CP-21/31/41` (Aug 2026) have **zero
shipments** — when you curate month M you are standing in M−1, and that month's kit has not shipped
yet. So modal ship month can *never* resolve the depth-0/depth-1 M−1 cell. This retires §10.1's
recommendation. Trust `age_rank` only for `r ≥ 65`; ranks 55/56 are out-of-sequence legacy kits.

Also: `kits.name` is unreliable — `OBB-CE-31 KITS` is named "OCT 2025 - CE-31" but ships in 2025-09.

### (b) T1 kits cannot be resolved by month at all — use recency rank

There are only **four T1 renewal kits in the entire database**: `AB11`(28), `BO11`(67), `BP11`(68),
`BQ11`(69). T1 renewals are made roughly **annually**, not monthly, so "the T1 kit of June 2026" does
not exist. `BQ11` has **zero shipments ever**; `BP11`'s 47 shipments spread across 11 months at 12.8%
modal concentration.

Their `age_rank` **absolute** values are meaningless for T1 (BP11's 68 would decode to 2024-06, but
it is named "JULY 2025 - BP-11"). Their **relative order** is chronologically correct, and selecting
the d-th most recent reproduces Sheena exactly:

| depth | curating | selector | result | matches her sheet |
|---|---|---|---|---|
| 1 | T2 | newest T1 renewal | **BQ11** | ✅ |
| 2 | T3 | 2nd newest | **BP11** | ✅ |
| 3 | T4 | 3rd newest | BO11 | (predicted) |

### (c) Welcome kits span many months — a month filter silently drops them

| kit | shipments | distinct months | modal | modal conc. |
|---|---|---|---|---|
| WKE1 (T1) | 258 | 15 | 2026-07 | 13.6% |
| WKH2 (T2) | 374 | 12 | 2026-07 | 19.0% |
| WKC3 (T3) | 358 | **30** | **2024-03** | 15.6% |

WKC3's modal month is 2024-03, which falls in **no** September-2026 window — a "built in month M"
rule would have **silently dropped WKC3 from the T3 block list**, which Sheena does block. Correct
selector: `is_welcome_kit = TRUE AND trimester = L`, take `max(age_rank)`, tie-break
`quantity_available > 0`. Yields WKE1 / WKH2 / WKC3 / AP41 — exactly her set.

> **Data hazard:** `'OBB-WK-G2 KIT'` (0 shipments) and `'OBB-WK-G2 KITS'` (747 shipments) both exist
> sharing `age_rank` 10008. A naive max-age_rank picker can select the empty one. Tie-break on
> `quantity_available`.

### (d) Match against Sheena's actual columns

- **SEPT CQ21 (T2): EXACT MATCH, 9/9.** WKH2, CP21, CO21/22/23/24, CN21, WKE1, BQ11. Zero missing,
  zero extra.
- **SEPT CQ31 (T3): all 13 of her columns reproduced, but 3 EXTRA** — CO22, CO23, CO24. She
  enumerated all four size variants for the **depth-0** T3 layer (`CO31/32/33/34`) but listed only
  `CO21` for the **depth-1** T2 layer. Since all variants share one `age_rank`, any month filter
  picks up all four automatically.
  **Genuinely ambiguous** — (i) clerical omission, so the union is right, or (ii) deliberate, so
  deeper layers block only the base size. The variants differ by ~2 items each, so it is a real
  behavioural difference. **See §12.6 — one question for Sheena.** Safe default meanwhile: block all
  variants (over-blocking is recoverable; under-blocking ships a duplicate).

### (e) Why the existing kit query cannot be reused

`run_monthly_report:661` loads `is_welcome_kit = False AND quantity_available > 0`. Blocking needs
**welcome kits** *and* **zero-stock kits** — a kit built in June blocks regardless of leftover stock.
The rebuild must issue its own `kits` query.

## 12.4b Corrected implementation shape

```
build_month(kit)      = 2024-03 + (kit.age_rank − 65) months        # only for age_rank >= 65
layer_window(d, M)    = {M, M-1, M-2, M-3}          if d == 0
                        {M-1, M-2, M-3}             if d == 1
                        {s, s-1, s-2} where s = M - (3d - 2)   if d >= 2

blocked_kits(T, M):
    for d in 0 .. T-1:
        L = T - d
        # 1. welcome kit of this layer — NEVER month-filtered
        add active welcome kit: is_welcome_kit AND trimester == L, max(age_rank),
                                tie-break quantity_available > 0
        # 2. renewal kits of this layer
        if L == 1:  add the (max(d-1, 0))-th most recent T1 renewal kit    # recency, not month
        else:       add every trimester-L renewal kit whose build_month ∈ layer_window(d, M)
                    (all size variants come along automatically — they share one age_rank)
    DO NOT USE = union of kit_items over those kits ; CAN USE = every other item
```

Month arithmetic must use an absolute month index (`year*12 + month-1`) so Dec→Jan rollover is free —
T4 curation reaches back to December of the prior year.

## 12.4 (superseded sketch) Implementation shape

```
build_month(kit)  = modal ship month of kit_sku in `shipments`
                    (fallback: 2-letter SKU prefix, CA=2025-05, +1 month per letter — §10.1)

blocked_kits(T, M):
    for d in 0 .. T-1:
        L = T - d
        months = [M, M-1, M-2, M-3]        if d == 0
                 [M-1, M-2, M-3]           if d == 1
                 [M-3d+2, M-3d+1, M-3d]    if d >= 2      # d=2 -> M-4,M-5,M-6
        for each kit K with trimester(K) == L and build_month(K) in months:
            include K, ALL its size variants, and the layer's welcome kit
    DO NOT USE = union of kit_items over those kits
    CAN USE    = every other item
```

Binary. No percentages, no risk tiers. `RISK_HIGH_THRESHOLD` / `RISK_MEDIUM_THRESHOLD`
(`curation_report.py:28-29`) have no counterpart in her process and get deleted, not tuned.

## 12.4c DESIGN SPEC (2026-08-15) — reviewed, but DO NOT implement as-is (see §12.4d)

A design pass plus three adversarial review lenses ran against this. The spec below is sound in
structure and was independently confirmed to reproduce Sheena's T2 sheet exactly — but the reviews
found **five defects**, two of which are real bugs in the spec itself. Fix those first (§12.4d).

**Additional verification this pass:**
- Epoch formula cross-checked against `kits.name` text: r=65 → "MAR 2024 - BM-\*", r=93 → "JULY 2026
  - CO-\*", r=94 → "AUG 2026 - CP-\*". Exact, no drift.
- T1 renewals confirmed live: `AB11`(28), `BO11`(67, qty 0), `BP11`(68, qty 10), `BQ11`(69, qty 50).
  age_rank DESC order is exactly BQ11 → BP11 → BO11 → AB11.
- `curation_report.html:309` / `:356` **hardcode** the badge text and only test `== 'HIGH'`, so the
  choice of non-blocked label (`NONE` vs `LOW`) has **zero template blast radius**.
- Dec/Jan rollover needs no special-casing: Python's `divmod` floors toward −∞, so
  `_month_str_add("2026-01", -1)` → `"2025-12"` falls out for free.

**Shape (condensed):**

```python
AGE_RANK_EPOCH_MONTH = "2024-03";  AGE_RANK_EPOCH_RANK = 65;  AGE_RANK_MIN_TRUSTED = 65

_month_idx(s)         = year*12 + (month-1)                 # absolute index, rollover-free
month_to_age_rank(s)  = 65 + (_month_idx(s) - _month_idx("2024-03"))

_window_months(depth, M):
    depth 0 -> [0,-1,-2,-3] ; depth 1 -> [-1,-2,-3]
    depth d>=2 -> start = -(3d-2); [start, start-1, start-2]

blocked_kits(T, M, kit_index):
    for layer in range(T, 0, -1):
        depth = T - layer
        if layer == 1:  take t1_renewal_sorted[max(depth,1) - 1]      # recency, never month
        else:           for m in _window_months(depth, M):
                            r = month_to_age_rank(m)
                            if r >= AGE_RANK_MIN_TRUSTED: add by_month[(layer, r)]
        add active welcome kit for `layer`                             # every layer, never month-filtered
```

`load_kits_for_blocking(db)` is called **once per run** (not per trimester) and is deliberately
unfiltered by `quantity_available` / `is_welcome_kit` — `run_monthly_report:661`'s in-stock query
stays untouched, since that one drives kit *assignment*, which legitimately must respect stock.

**Backwards compatibility — recommended approach:** add `cycle_month: Optional[str] = None`. When
`None`, run the legacy percentage path verbatim. `projection_engine.py` needs **zero changes**: it
calls with exactly the five existing kwargs, so it keeps the legacy path, and Mode B's `extra_blocked`
union (`:141-149`) — which writes into `customer_items_in_window` — keeps working untouched.

**This is deliberate, not laziness:** the Forward Planner projects 3–6 months ahead, and the kits that
*would* be blocked in those future months **do not exist in the DB yet** (CQ/CR/CS have no rows). Under
kit-recipe blocking a future month would return a partial-then-empty block list, producing a visible
"cliff" between horizon months that reflects data availability, not real risk — actively misleading on
a view Ting and Sheena read as forward guidance. A `None` sentinel that *never computes a substitute
month* also avoids the trap where every horizon month silently resolves to the same window.

## 12.4d FIVE DEFECTS FOUND IN REVIEW — fix before implementing

**D1 — CRITICAL (spec bug): the welcome-kit tiebreak selects the WRONG row.**
The spec's prose says the final tiebreak is `sku` DESC, but the code is
`min(candidates, key=lambda k: (-age_rank, -qty, k["sku"]))` — `sku` **ascending**. Verified in
Python: `"OBB-WK-G2 KIT" < "OBB-WK-G2 KITS"` is `True`, so `min()` picks the **zero-shipment**
duplicate — the exact opposite of the stated safety intent. Both rows carry a full 8-item recipe, so
when it fires it blocks the wrong 8 items. Dormant today only because WKH2 (10009) wins outright.
The same inverted key sits in `t1_renewal_sorted`. **Fix:** use `max()` with an un-negated key.

**D2 — HIGH: the empty-trimester early return is no longer correct.**
`if group_size == 0: return [], []` fires *before* `cycle_month` is inspected. That was right under
the percentage rule (blocking depended on the customer group); under kit-recipe blocking the block
list **does not depend on customers at all**. Not hypothetical: T1's live pool is already down to
**6 customers**, so a zero month is realistic — and when it happens `curation_run_items` silently
gets zero rows for that trimester even though `blocked_kits(1, "2026-09")` returns a real
`{BQ11, WKE1}`.

**D3 — MEDIUM: `app.py:5741` sort collapses.** The saved-report view re-sorts `do_not_use` by
`-blocked_pct`, which under the binary rule is a **constant 100.0** for every row — so on-screen order
degenerates to whatever order PostgREST happens to return. The design audits the duplicate predicate
at `:5735` but stops one line short of `:5741`. Needs a secondary key in the same change set.

**D4 — MEDIUM: the "X / Y" column becomes self-contradictory.** `curation_report.html:298` renders
`blocked_count / group_size` (e.g. "3 / 9") immediately beside `:301`'s `blocked_pct` (now always
"100.0%"). A reader sees "3 / 9 → 100.0%" and reasonably calls it a bug. This is the *same* pattern
the spec proudly fixes for expired items, reintroduced for every ordinary blocked item. Sheena and
Ting are the actual audience for this table — decide the display before shipping.

**D5 — LOW (accuracy of a claim, not code): the epoch verification overclaims.** "Verified against
`kits.name` for r=65→94" — but `kits.name` is **NULL for all 9 rows at age_rank 91 (`CM`) and all 3 at
92 (`CN`)**, so 12 of 30 ranks had no text to check. The sequence is unbroken and neighbours (90 = APR
2026, 93 = JULY 2026) make 91/92 = MAY/JUN 2026 near-certain by continuity — but that is inference, not
confirmation. These ranks are not idle: `blocked_kits(4, "2026-06")` puts both `CM-41` and `CN-41`
into the blocked set.

**Also flagged, not blocking:** no log line if a blocked kit resolves to an empty recipe (5 of 200
kits have zero `kit_items`, all with `age_rank < 65` so excluded today) — contrary to CLAUDE.md's
logging standard.

## 12.4e Confirmed SOUND under review (checked, not assumed)

- **T2 (SEPT CQ21): EXACT 9/9**, hand-walked against live rows.
- **No hard breaks anywhere** — Forward Planner, `_save_report_details`, Sheets export, and the
  template's risk-level/threshold logic all verified against live schema and actual call sites. No
  KeyError, no CHECK violation, no NUMERIC(5,2) overflow, no tuple-shape mismatch. `'NONE'` correctly
  falls to `can_use` at `app.py:5735` with zero code change.
- **T4 across the Dec/Jan boundary works:** `blocked_kits(4, "2026-06")` → T2 layer at depth 2 →
  `"2025-12"` → rank 86 → the real `OBB-CH-21/22/23/24` rows named "DEC 2025 - CH-2x".
- **T1 curation, missing-month cells, and the not-yet-existing cycle-month kit** all resolve to empty
  lists without error. `month_to_age_rank("2026-09") == 95`; zero kit rows exist at 95.
- **`item_alternatives` expansion should survive** — it models physical substitutability, orthogonal
  to how the blocked set is derived. Only 8 rows, and dropping it risks under-blocking a registered
  duplicate-equivalent.

## 12.4f The CO22/23/24 ambiguity is REAL, and it is not cosmetic

Quantified at the item level: all four `CO-2x` rows share `age_rank 93`, each has exactly 8 items,
and **CO-22, CO-23 and CO-24 each contribute exactly 1 item not present in CO-21.** So the T3 run
would carry up to 3 genuinely distinct extra items into DO-NOT-USE.

**Structural, not a coding slip:** layer 2 at depth 1 (inside a T3 curation) and layer 2 at depth 0
(a T2 curation) hit the *identical* `by_month[(2, 93)]` bucket. Nothing in the data model can
distinguish "block CO-21 only" from "block all CO-2x" at different depths without a depth-conditioned
variant filter.

**Safe default: keep the union** (over-blocking costs a kit slot; under-blocking ships a duplicate).
**Ask Sheena — and ask for the general rule, since this recurs for every multi-variant prefix at every
depth ≥ 1:** *"Your T3 sheet's T2-layer row lists only CO21, but the T3-layer row above lists all four
CO31/32/33/34 — was that shorthand for the whole CO-2x block, or deliberate (deeper layers block only
the base size)?"*

## 12.5 Acceptance test — we already know the right answers

Curating **September 2026**, the implementation must produce exactly:

| Curating | Expected blocked kits |
|---|---|
| T2 | WKH2, CP21, CO21(+22/23/24), CN21, WKE1, BQ11 — and **not** BP11 |
| T3 | WKC3, CP31, CO31(+32/33/34), CN31, WKH2, CP21, CO21, CN21, WKE1, BP11 — and **not** BQ11 |

The T2 row matches her spreadsheet columns exactly (including BP11 being deleted, 5:25). The T3 row is
the §12.3 prediction. If T3 comes out with `BQ11` in it, either the rule or our build-month resolution
is wrong — investigate rather than accepting it.

## 12.5b Implementation checklist (nothing built yet — this is the ordered to-do)

1. Fix **D1** (welcome-kit + T1 tiebreak → `max()`, un-negated key) before anything else.
2. Fix **D2** (move the empty-trimester early return *after* the kit-recipe branch, or drop it there).
3. Decide **D4**'s display, then implement `blocked_kits` + the `generate_item_risk_report` rewrite.
4. Fix **D3** (`app.py:5741` secondary sort key) in the same change set.
5. Re-verify against §12.5's acceptance test — T2 must be 9/9 exact; T3 will be 13/13 + the 3 CO
   variants until Sheena answers §12.4f.
6. `projection_engine.py`: **no changes** (legacy path via `cycle_month=None`).
7. Deferred, needs its own authorisation: the `EXPIRED` badge — `risk_level` is CHECK-constrained to
   `HIGH/MEDIUM/LOW/NONE`, so it needs migration **021** *plus* an `app.py:5735` widen *plus* a
   template badge change, and CLAUDE.md forbids touching templates outside a UI task. The interim win
   is free: expired items go from "0/N · 0.0% · HIGH" to "0/N · 100.0% · HIGH", which at least stops
   reading as a bug.
8. Data hygiene, separate: the `OBB-WK-G2 KIT` / `OBB-WK-G2 KITS` duplicate pair should be cleaned up
   in Supabase directly.

## 12.6 Still open

Only §11.2 remains: does her 407 (12/139/142/114) include first-time customers? The Aug 3 meeting and
her 2026-08-13 Slack reply contradict each other. Do not tune the pool against 407 until settled.

---

# 13. IMPLEMENTED (2026-08-15) — §12.5b's checklist, built and audited

All 8 items in §12.5b's checklist are done, plus §10.9's other backend-only "ready to build" items.

**`curation_report.py`:** added `load_kits_for_blocking()`, `blocked_kits()`, `compute_blocked_items()`,
the `_month_idx/_month_add/month_to_age_rank/_window_offsets` helpers, and
`_generate_item_risk_report_kit_recipe()`. `generate_item_risk_report()` gained an optional
`cycle_month`/`kit_index`/`kit_items_map` param set — D1 (tiebreak) and D2 (empty-trimester early
return ordering) both fixed in the new path. `run_monthly_report()` now always calls it with
`cycle_month=report_month`, so the live monthly report is 100% kit-recipe blocking. The legacy
percentage path (`cycle_month=None`) is untouched byte-for-byte except the D3 sort-tiebreak fix, and
stays load-bearing: `projection_engine.py`'s Forward Planner calls `generate_item_risk_report()` with
no `cycle_month` (it has no kit build-month for future months — see §12.4c), so it silently keeps
using the legacy branch. Zero changes to `projection_engine.py` itself. Also ported the kit-stock
coverage cap from `projection_engine.py:366-376` into `run_monthly_report`'s step 8a (§10.9 item 6).

**`app.py`:** D3 sort-tiebreak fix at the saved-report view (~5741). Scheduler trigger moved day 1 → 3,
`pool_source="shipments"` pin dropped so the unattended run now uses the new pool too (§10.9 item 2,
Hasan's 2026-08-13 decision).

**Verification:**
- Live read-only script against the real DB: `blocked_kits(2, "2026-09", ...)` = the exact 9 kits
  §12.5 predicts (WKH2, CP21, CO21/22/23/24, CN21, WKE1, BQ11 — no BP11). `blocked_kits(3, "2026-09", ...)`
  = the predicted 13 + the documented CO22/23/24 over-block (§12.4f) = 16 kits, no BQ11. Exact match,
  both trimesters.
- Full read-only `run_monthly_report(report_month="2026-08")` end-to-end: every DO-NOT-USE item across
  all 4 trimesters has `blocked_pct == 100.0` and `risk_level == 'HIGH'` (binary, confirmed live, not
  just by code inspection). T1 (6 live customers) still produced a real 30-item block list — the D2 fix
  holds under real data, not just the empty-pool edge case.
- `tests/test_engine.py`: 6/6 pass. (`tests/test_phase2.py` can't even be collected without a running
  local server — pre-existing, unrelated to this work, not touched.)
- Independent Sonnet-subagent audit (adversarial, hand-traced against the plan's own stated live-data
  facts): verdict CORRECT. Found one cosmetic bug — `_monthly_report_scheduler`'s docstring still said
  "checks... if it's the 1st" after the trigger moved to day 3. Fixed.

## 13.1 Stock-aware kit allocation (2026-08-15) — replaces the aggregate cap

The ported §3.5 cap (`covered <= total trimester stock`) turned out to be the wrong denominator and
was replaced the same day. Two Sonnet audits plus live measurement agreed:
`evaluate_existing_kit_coverage()` is stateless per customer, so a kit with 2 units was recommended
to every customer whose history pointed at it. Measured on run `f7842b2a`: BT-32 (stock 4) → 45
customers, BQ-41 (stock 26) → 34, BP-41 (stock 2) → 9, BT-21 (stock 3) → 29, BC-21 (stock 100) →
105. **121 customers were promised a kit that did not physically exist.** The aggregate cap never
fired because each trimester's total stock was comfortably above its covered count — a big recent
kit (CP-31: 200, CP-41: 150) carried the slack while small kits were exhausted.

Replaced with a single allocation pass holding `remaining_stock` per kit, assigning each customer
their oldest safe kit that still has units and falling through their full safe list when one runs
out. `evaluate_existing_kit_coverage()` now also returns `safe_kit_ids` (the complete ordered list,
not just the 3 alternatives surfaced in the UI) so the caller can reallocate. The aggregate cap is
gone — per-kit allocation subsumes it.

Result (2026-08, ship 14): **zero over-promised kits in every trimester.** T2 136 covered (now
spread over 5 kits, BC-21 capped at exactly its 100), T3 151 → **141** (10 customers have no
in-stock safe kit), T4 43 covered but redistributed onto idle BT-41/BV-41 stock instead of
oversubscribing BQ-41/BP-41. Build quantities unchanged this month only because the 100-unit
warehouse minimum dominates; in a month where it doesn't, the old numbers would have been too low.

**Open policy question, defaulted not decided:** when a scarce kit runs out, who gets it? The pass
processes customers fewest-safe-kits-first (so a scarce kit isn't consumed by someone who had other
options), tie-broken on `customer_id` for determinism. That maximises coverage but is an engine
default, not a stated business rule — if Sheena has a real priority order (longest subscribed,
earliest due date), it belongs in that sort key.

**Deliberately not done, per CLAUDE.md ("never touch templates unless the task is specifically UI") and
"never guess, ask Hasan" — flagged, not silently skipped:**
- D4's display decision and the EXPIRED badge (§12.5b item 7) both require a `templates/` change (the
  EXPIRED badge also needs migration 021 for the `risk_level` CHECK constraint). Needs an explicit
  UI-task go-ahead.
- Ship day 14 → 15 (§3.4): still only "weakly supported" per the plan's own words — not a confirmed
  standing rule. Left at 14.
- Rendering `renewal_by_platform` (already computed, §9.4): same template-touching constraint as D4.
- §11.2 (Sheena's 407 provenance) and the CO22/23/24 union-vs-base-size question (§12.4f): both
  genuinely open business-rule questions for Sheena, not implementation blockers — unchanged from §12.6.
