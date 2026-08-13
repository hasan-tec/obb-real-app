# July 2026 Import Plan

**Status:** v2 — audited by an adversarial subagent against the code, the schema, the CSVs and the
live DB. Audit verdict: **SAFE WITH FIXES**. The fixes are folded in below.
**Scope:** import the July 2026 renewals Sheena supplied (5 CSVs) into `customers`, `shipments`,
`shipment_items`. Prerequisite for `CURATION_REBUILD_PLAN.md` §4.1.

Every number here was measured read-only against the live DB. Where a number came from the script's
own summary rather than a real measurement, that is now stated explicitly.

---

## 0. The hard rule this plan obeys

**Nothing in this plan writes to `kits` or `items`. Ever.**

Sheena creates the month's kit and item rows herself in the app. The importer's only interaction
with those tables is a **read** to resolve a SKU to an existing row id.

**Half of this is enforced in code today; half is not.**

**Enforced (audit-verified, all seven write sites traced):** `scripts/import_history.py` writes only
to `customers` (`:755`, `:770`, `:826`), `shipments` (`:913`, `:954`) and `shipment_items` (`:931`,
`:962`). No `upsert`, no `delete`, no `rpc`. `load_kit_cache()` (`:410`) is `SELECT`-only. The five
insert-on-miss seeders (`seed_old_kits`, `seed_cm_cn_kits`, `seed_cratejoy_missing_kits`,
`import_wk_history`, `fix_orr_items`) all `sys.exit(2)` behind `scripts/_legacy_seed_guard.py`.

**NOT enforced:** an unresolved SKU does **not** halt the run. `:881-886` logs a warning and inserts
the shipment as text-only with no `kit_id` and no items. Worse, `:422-424` swallows any exception
from the kit-cache load and returns an **empty** `KitCache`, and `:1077-1081` only warns. A transient
Supabase blip in Phase 0 would therefore produce 358 shipments with no items and a non-canonical
`kit_sku` — silently, reported as success. **Step 3 fix #3 closes this.** It does not affect the
kits/items rule (nothing is ever created), but the plan must not claim a guarantee the code lacks.

---

## 1. What the files actually contain (measured)

| File | Rows | Kit groups | Identity column |
|---|---|---|---|
| `...JULY '26 CO.csv` (Shopify) | **358** customer rows, 352 distinct emails | 13 | **email** OK |
| `JULY RENEWALS - CRATEJOY T1.csv` | 1 | `OBB-BP-11 Kits` | name only — NO EMAIL |
| `...CRATEJOY T2.csv` | 27 | CO21–CO24 | name only — NO EMAIL |
| `...CRATEJOY T3.csv` | 40 | CO31–CO34 | name only — NO EMAIL |
| `...CRATEJOY T4.csv` | 19 | CO41–CO44 | name only — NO EMAIL |
| | **445 total** | | |

Two structurally different file families. The Shopify file is the standard 23-column export the
existing importer already understands. The Cratejoy files are a hand-built 8-column layout with
**no email column at all**.

### Kit SKU resolution — all 13 codes verified present

The proposed rule is correct and **already implemented** by `parse_kit_sku()`
(`scripts/import_history.py:246`). Audit tested the functions directly:

```
parse_kit_sku('CO41')           -> 'OBB-CO-41 KITS'   (regex ^([A-Z]{2})(\d{2})$ -> OBB-\1-\2 KITS)
parse_kit_sku('OBB-BP-11 Kits') -> 'OBB-BP-11'        (strip " Kits"; KitCache resolves to the KITS row)
```

Verified live against `kits` — **13 of 13 match, zero missing**:

| CSV code | DB row | T | items linked |
|---|---|---|---|
| CO21 / CO22 / CO23 / CO24 | `OBB-CO-2x KITS` | 2 | 8 each |
| CO31 / CO32 / CO33 / CO34 | `OBB-CO-3x KITS` | 3 | 8 each |
| CO41 / CO42 / CO43 / CO44 | `OBB-CO-4x KITS` | 4 | 8 each |
| `OBB-BP-11 Kits` | `OBB-BP-11 KITS` | 1 | **9** (not 8) |

`extract_trimester_from_sku()` returns the right trimester for all 13 — `'OBB-CO-41 KITS' -> 4`
works because `part[0].isdigit()` only inspects the first character of the `"41 KITS"` segment.

**So: no seeding needed, and the item lists under each kit heading are ignored entirely.**
`shipment_items` is populated by copying the kit's existing `kit_items` rows — never by parsing item
names from column A. String-matching item names is fragile and would risk creating items.

> Fragility worth knowing: `KitCache` resolves `'OBB-CO-41 KITS'` by **exact** match. Every DB kit is
> spelled uppercase `KITS`, so this works — but it would break silently if a kit were ever entered
> as `Kits`. Note there is already an `OBB-WK-G2 KIT` / `OBB-WK-G2 KITS` pair in the table.

---

## 2. Blockers and data-quality issues

### 2.1 BLOCKER — Cratejoy rows cannot be identified

`customers` has `email TEXT NOT NULL` and `UNIQUE INDEX ON LOWER(email)`. The Cratejoy files carry
no email, only a display name. Matching the 87 names against 2,486 customers (audit reproduced this
split exactly):

| Match tier | Count | Verdict |
|---|---|---|
| Exact name, single hit | 26 | safe — all 26 are `platform='cratejoy'`, which corroborates them |
| Last name + due date, single hit | 15 | needs human confirmation |
| Due date alone unique | 2 | **reject** — a shared due date is not identity |
| Same name, multiple customers | 1 | ambiguous (`Nicole Bruton`) |
| **No match at all** | **43** | **blocked** |

Only **26 of 87 (30%)** are safely importable. Tier 2 pairs rows like `'Emily VanCampen'` to
`sheilajohnson65@…` and `'Bre Brown'` to `kristy@madiganestates.com` — *plausible* for Cratejoy gift
subscriptions where the account email is the purchaser's and the name is the recipient's, but
"plausible" is not good enough to write 15 people's shipment history to the wrong account.

**Do not fuzzy-match through this.** The clean fix is one Slack message: ask Sheena to re-export the
four Cratejoy files with the subscriber **email** (or Cratejoy customer/subscription id). That turns
all 87 into exact matches and removes the guesswork permanently.

The 43 no-match rows are consistent with the known Cratejoy inbound-sync gap (webhooks dead since
6/24; ~94 customers never created) — they likely do not exist in the DB at all.

### 2.2 The 6 duplicate emails — risk was OVERSTATED in v1

Six emails have two different orders, kits, sizes and due dates:

| Email | Order A | Order B |
|---|---|---|
| `leaannkirby@…` | CO42, med, due 2/11/2026 | CO33, l, due 10/16/2026 |
| `miss_tey20@…` | CO43, l, due 7/6/2026 | CO44, xl, due 6/14/2026 |
| `agiannakas@…` | CO31, sm, due 8/13/2026 | CO33, l, due 10/6/2026 |
| `sixcovs@…` | CO32, med, due 10/9/2026 | CO34, xxl, due 8/17/2026 |
| `theresalvaros@…` | CO33, l, due 9/30/2026 | CO21, sm, due 12/4/2026 |
| `jessicamariehawley@…` | CO22, med, due 1/13/2027 | CO24, xl, due 12/18/2026 |

These read as two subscriptions on one email (gifting, or two recipients).

**Correction from the audit — the merge never reaches the DB.** v1 claimed "6 customers would land
in the wrong trimester bucket." They cannot. `update_existing_customers()` guards with
`if new_data.get(field) and not existing.get(field)` (`:808-810`), and all six already have non-null
`due_date` and `clothing_size`. This import writes neither field for any of the 352.

The genuine, smaller issue is the **inverse**: three of the six already hold the *other*
subscription's due date in the DB, and this import will not correct it. Both shipments still import
correctly (kit SKUs differ). So it is a reporting gap for Sheena to adjudicate, not a corruption
risk. (Also note `normalize_size` collapses `xxl -> XL` at `:183` — the CHECK constraint only permits
S/M/L/XL, so `sixcovs@…`'s "xxl" is not representable either way.)

### 2.3 One corrupt due date — and it is ALREADY LIVE

Line 20 has `10/6/0226` — year **226**. `parse_due_date()` accepts it as a valid `%m/%d/%Y` parse.

**Already in the database:** `kabbs555@gmail.com` currently has `customers.due_date = '0226-10-06'`.
The proposed parser bound is therefore *prophylactic only* — because the field is non-null, the
null-fill guard means this import would never have touched it. **A separate one-row repair is
required**, and §4 must verify it.

Same defect class in the Cratejoy files: T4 `Rob Nielsen` has due `2025-07-05`, and a tier-3 row has
`2022-07-19` — both in the past for a July 2026 renewal.

---

## 3. The plan

Steps 1–4 are Shopify-only and unblocked. Step 5 is Cratejoy and is blocked on §2.1.

### Step 1 — Stage the Shopify file — **DONE**
Copied to `C:\Users\hasan\Desktop\opus tin\order history\` — note this is the **workspace root**,
not `opus-obb-prototype/order history`, because `ORDER_HISTORY_DIR` is `SCRIPT_DIR.parent.parent`
(`:140`). Byte-identical to the Downloads copy.

No collision: `JULY '24 BQ.csv` and `JULY '25 CC.csv` exist, but `--file "JULY '26 CO"` matches
exactly one file and `batch_sort_key` returns 66 (`CO`), unique among the 28 files.

> **Do NOT stage the Cratejoy CSVs here.** `extract_month_meta_safe("JULY RENEWALS - CRATEJOY
> T1.csv")` finds no `'26` and defaults to **year 2025**; `batch_sort_key` returns 9999. A future
> `--all` would sweep them in. They'd parse to 0 customer rows (col 2 is a date, not an email), so
> no writes — but it is a trap worth avoiding.

### Step 2 — Dry run, unmodified script — **DONE, PASSED**
```bash
python scripts/import_history.py --dry-run --file "JULY '26 CO"
```
Actual result (run 2026-08-12, nothing written):

```
KIT GROUP -> OBB-CO-41/42/43/44 KITS      (13 groups, all resolved)
KIT GROUP -> OBB-CO-31/32/33/34 KITS
KIT GROUP -> OBB-CO-21/22/23/24 KITS
KIT GROUP -> OBB-BP-11
-> 358 customers collected, 0 skipped
-> 352 unique customers | 0 new | 352 existing

Customers CREATED:              0
Customers UPDATED (null fields):352      <-- upper bound, NOT a measurement
Shipments CREATED:              358
Kit SKUs not found in DB:       0
Errors: 0     Warnings: 0
```

**Correction to my own estimate:** I expected ~352 customer *inserts*. There are **zero** — all 352
emails already exist. Materially safer than v1 stated.

**That 352 "UPDATED" figure is the dry run's upper bound, not a measurement** — `:1117` sets it to
`len(upd_emails)` and the code's own comment says so. The audit simulated
`update_existing_customers()` against live data: **149** customers actually get an UPDATE, writing
`phone` ×148, `address_line2` ×2, `baby_gender` ×1. **Zero `due_date`, zero `clothing_size`, zero
`platform` writes.** No existing good data is clobbered.

**`Shipments SKIPPED (duplicate): 0` proves nothing about idempotency.** A dry run returns at `:1141`,
before `load_existing_ship_keys()` is ever called at `:1192`. Its duplicate estimate (`:1120-1128`)
keys on the *raw* CSV SKU and email, in memory only. The 0 means "no duplicates within the file" —
true, 358 distinct keys of 358 rows.

Gate passed: "Kit SKUs not found in DB: 0" confirms SKU normalisation works end to end against live
data with no code change.

*Dry-run reporting gap:* "Shipment items linked: 0" — that counter is only incremented on the live
path.

### Step 3 — Three guard changes to `import_history.py`
The first two are reporting-only. **The third is a real safety fix the audit forced.**

1. **Sanity-bound `parse_due_date()`** — reject a parsed year outside 2000–2100, log a warning,
   return `None` rather than storing `0226-10-06`. Prophylactic (see §2.3).
2. **Warn on conflicting merge** — in `collect_from_csv()`, when an email already has a `due_date` or
   `clothing_size` and the new row carries a *different* non-null value, log a warning naming the
   email and both values. Surfaces §2.2's 6 rows.
3. **Abort on unresolved SKU or empty kit cache** — before Phase 3, `sys.exit(1)` if
   `stats.kits_not_found_in_db` is non-empty **or** `len(kit_cache) == 0`. This is what makes §0's
   stated rule true in code rather than in prose, and it closes the silent-empty-cache path at
   `:422-424` / `:1077-1081`.

Nothing else changes. The script already handles both kit formats, trimester extraction, the
23-column layout, dedup and batching correctly.

### Step 4 — Re-run dry run, then live (Shopify only)
Re-run step 2 and confirm: 1 due-date rejection, 6 merge-conflict warnings, 0 missing kits.
**Then stop and show the output before anything live runs.**

> **Enforce that stop manually.** The `input()` gate at `:1151-1158` catches `EOFError` and proceeds
> automatically in any non-TTY context, so launching the live run through a wrapper or agent tool
> would self-approve it. Run it from an interactive shell.

Live run writes (measured, not estimated):
- **0 inserts** to `customers`; **149 updates** filling only `phone` / `address_line2` / `baby_gender`
- **358 inserts** to `shipments`
- **2,866 inserts** to `shipment_items` — 356×8 plus the two `OBB-BP-11 KITS` rows at **9** items each
- **0 rows to `kits`. 0 rows to `items`.**

Do not verify the item count from the summary counter: `:927-939` increments `ship_items_linked`
outside the try/except, so failed batches still count as linked, and the fallback path swallows item
errors at `:966-967`. Count the table.

**Idempotency — true under normal conditions, with three known holes.** `:869` canonicalises the SKU
before building the dedup key and `:890` writes that same canonical string, so
`load_existing_ship_keys()` (`:485-488`) reads back identical keys and a second run skips all 358.
Audit verified all 13 SKUs round-trip to exact DB spellings. The three holes, none of which abort
the run today:

- (a) `load_existing_ship_keys()` `break`s on a page error (`:479-482`) and returns a **partial** set
  — `shipments` is 11,488 rows across 12 pages.
- (b) An empty `KitCache` from a transient Phase-0 failure changes the key for the BP-11 group.
  *(Step 3 fix #3 closes this one.)*
- (c) A batch insert that commits but whose HTTP response fails triggers the per-row fallback at
  `:951-972`, re-inserting the whole batch.

There is **no unique constraint on `shipments`** to catch any of these — `001` and `017` create plain
indexes only. Dedup is application-level only. Treat a failed run as needing manual inspection, not
a blind re-run.

### Step 5 — Cratejoy (BLOCKED)
Do not build until Sheena supplies emails. When she does it is a small new script
(`scripts/import_july_cratejoy.py`) reusing the same `KitCache`, the same dedup key, and
`platform="cratejoy"` — the only genuinely new logic is parsing the 8-column layout.

If she cannot re-export with emails, the fallback is to import **only the 26 exact-name matches** and
hand back the other 61 for her to fill in. Partial and honest beats complete and wrong.

---

## 4. Verification

**0. Record pre-run baselines first** (audit-measured, 2026-08-12):

| Table | Baseline |
|---|---|
| `items` | **355** |
| `kits` | **200** |
| `shipments` | **11,488** |
| `shipment_items` | **95,556** |
| `customers` | **2,486** |

1. `shipments` with `ship_date` in July 2026 → expect 133 existing + 358 new = **491**. The 133 are
   all welcome kits (`WK-H2` 71, `WK-E1` 35, `WK-C3` 16, `WK-E2` 8, `AP-41` 3), all
   `is_welcome_kit=true`, zero non-welcome July SKUs — simulating the real dedup key against live
   data gives **0 collisions**.
2. **`kits` unchanged at 200. `items` unchanged at 355. This is the rule check.**
3. `customers` unchanged at **2,486** (0 inserts expected).
4. `SELECT count(*) FROM customers WHERE due_date < '2000-01-01'` → must be **0** after the §2.3
   one-row repair.
5. Spot-check 5 customers across different kit groups against the CSV by hand.
6. Re-run the Aug 2026 curation report and confirm the T1–T4 pool counts move as expected.

---

## 5. Open questions for Sheena

1. Can the four Cratejoy files be re-exported **with subscriber email**? (Unblocks 61 of 87 rows.)
2. The 6 duplicate-email customers in §2.2 — two subscriptions on one account, or a data error?
   Which due date and size is correct for each?
3. Line 20's due date reads `10/6/0226` — should be `10/6/2026`? (Already live in the DB.)
4. Cratejoy `Rob Nielsen` has due date `2025-07-05`, in the past for a July 2026 renewal. Correct?

---

## 6. Non-goals
- Creating any `kits` or `items` row, under any circumstance.
- Parsing item names from column A — `kit_items` is the source of truth for shipment contents.
- Backfilling the ~94 missing Cratejoy customers — separate task, tracked in `BACKFILL_PLAN.md`.
- Adding a unique constraint to `shipments`. Worth doing, but it is a schema change outside this
  import's scope — logged here so it is not forgotten.

---

## 7. Audit trail
Adversarially audited 2026-08-12 by a subagent with read-only DB access, which independently
re-derived every count, traced all seven write sites, tested `parse_kit_sku` /
`extract_trimester_from_sku` / `extract_month_meta_safe` / `batch_sort_key` directly, and checked
migrations 001–020 for constraint violations. Verdict **SAFE WITH FIXES**; all fixes folded in above.
Corrections it forced: the halt-on-unresolved-SKU guarantee (§0), the 352→149 update count (Step 2),
the overstated §2.2 risk, the already-live corrupt due date (§2.3), the 2,864→2,866 item count, the
meaninglessness of the dry run's duplicate counter, and the three idempotency holes (Step 4).
