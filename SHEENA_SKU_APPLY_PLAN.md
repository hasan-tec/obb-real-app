# Applying Sheena's Confirmed VeraCore SKUs — PLAN v2 (rewritten 2026-07-29)

> **v1 was withdrawn.** It was built on a grouping bug and contained a destructive instruction.
> Every number in this v2 was recomputed from scratch and **independently reproduced twice**
> (my re-derivation + an 11-agent adversarial audit) — the two agreed exactly.
> **Status: nothing has been written to the database. No phase has been run.**

**Inputs**
| File | Role |
|---|---|
| `VeraCore SKU Confirmation - OBB correctly filled by sheena.xlsx` (2026-07-29) | Sheena's answers, 153 rows |
| `REEXPORTED-ProductSummary-639191819179040822.xls` (2026-07-09, **1,140** products) | ✅ the one to use |
| `ProductSummary-639191807228604742.xls` (2026-07-09, 273 products) | ❌ strict subset of the above — ignore |

---

## 0. What changed from v1, and why it mattered

| v1 said | Truth | Consequence had we run it |
|---|---|---|
| 49 groups / 107 rows / 58 retired | **50 / 110 / 60** | apply script retires the wrong rows |
| "mirror the `…CleansingOiol` typo exactly, never fix it" | `OIOL` = **0 hits** in 1,140 products | writes a **non-existent SKU**, permanently breaks that item's sync |
| `curation_run_items` — "CASCADE ✅" | **34,002 live rows; ~3,678 destroyed** | silent loss of curation history, no error raised |
| sandbox "13/13 PASSED — algorithm proven" | **9/13** — 3 tests hardcoded `True`, 1 ran on an empty list | the merge algorithm was **never actually proven** |
| #71 "two items sharing `veracore_sku` → stock to both" | dict comprehension = **last-wins** | exactly one twin gets stock, silently |
| survivor = most kit links | wrong in **5 groups** | deletes rows holding **1,835 shipments** |
| a CHAIN exists (MamaCap) | **0 chains** — artifact of matching only Product Id | — |

**Root cause of most of it:** Sheena sometimes pasted the **Description** column, not the Product Id.
Resolving answers against **Product Id → then Description** removes all chains and 7 of the 10
"missing" answers.

---

## 1. Verified ground truth

### 1.1 Live DB (paginated; reproduced twice)
| Table | Rows |
|---|---|
| `items` | **405** (`veracore_sku` set on **0**) |
| `kit_items` | **1,595** (100% `quantity=1`) |
| `shipment_items` | **95,139** |
| `shipments` / `customers` / `kits` | 11,438 / 2,437 / 197 |
| `item_alternatives` | 8 |
| `curation_committed_items` | **0** (FK has **no** CASCADE — would hard-fail if ever non-zero) |
| `curation_run_items` | **34,002** (FK **CASCADE** → silently deleted) |

### 1.2 Schema (read from migrations)
- `items.sku TEXT UNIQUE` (`001:48`) ← the entire collision problem
- `items.veracore_sku TEXT`, **no constraint** (`001:53`) ← the safe column
- 6 FKs reference `items(id)`: 5 `ON DELETE CASCADE`, 1 (`curation_committed_items`, `008:9`) with none
- `kit_items` PK `(kit_id,item_id)`; `shipment_items` PK `(shipment_id,item_id)`

### 1.3 Rename safety — CONFIRMED
- `kit_items` / `shipment_items` / `item_alternatives` link by **`item_id` UUID only**. Renaming
  `items.sku` **cannot** break kit composition or customer history. Verified by grep: zero lookups by sku/name.
- Engine duplicate check is a pure UUID set intersection (`assign_kit`, **`app.py:1983`** — v1's `1775` was stale).
- **`app.py:1648` is the only name-based item lookup in the repo** (`resolve_history_item_ids`).
  It is `.eq("sku", ref.upper())` — case-sensitive, so it will miss the new mixed-case SKUs.
  Blast radius **133 rows / 29 shipments**. Fix to `.ilike`; not a gate.

⚠️ **All `app.py` line numbers in v1 were stale by 200–900 lines** (file moved 2026-07-20). Re-verify any citation before trusting it.

---

## 2. The corrected model

**Rule: resolve every answer against Product Id FIRST, then Description → Product Id. Never normalize whitespace** (two real Product Ids contain spaces Sheena removed).

Resolution of the 150 DB-resolvable rows: **140 via Product Id · 7 via Description · 3 unresolved.**

| Class | Count |
|---|---|
| `CLEAN_RENAME` | **77** |
| `MERGE_INTO_EXISTING` | **51** |
| `MERGE_NEW_TARGET` | **18** |
| `CHAIN` | **0** |
| `CYCLE` | **0** |

**Merge totals: 50 groups · 110 member rows · 60 retirements.**
Repoint **95 `kit_items`** + **3,093 `shipment_items`**. Cascade-destroys **~3,678 `curation_run_items`**.

> Correcting the sheet makes the blast radius **bigger** (60 retirements, not 58). Any script built on 57/58 is wrong.

### 2.1 The 3 truly-unresolved answers (real Product Ids identified)
| Sheena wrote | Real Product Id | Note |
|---|---|---|
| `OBB-Skin&Co+TruffleTherapyCleansingOiol` | `OBB-Skin&Co+TruffleTherapyCleansingOil` | her typo — **fix it** |
| `OBB-BirthkeepingByBekah+PregnancyTea` | `OBB-BirthkeepingByBekah+Pregnancy Tea` | **space required**; beware sibling `+HealthyMilkTea` |
| `OBB-NorthernSole+LaborGripSocks(White+Blue)` | `OBB-NorthernSole+LaborGripSocks (White+Blue)` | **space required** |

**The genuine VeraCore-side typo is a different row:** `OBB-LoveKnitBaby+PeekabooUltrasoundPFrame&Magnet`
(stray `P`) — **mirror that one exactly**.

---

## 3. Known hazards that must be fixed in code, not just planned around

| # | Hazard | Evidence | Required action |
|---|---|---|---|
| H1 | **Staff edit silently reverts the work.** `sku.strip().upper()` on add/edit/quick-add, and `templates/items.html` always resubmits `sku` — so editing *unit_cost* re-uppercases the SKU | 3 call sites | stop force-uppercasing, or exclude `sku` from resubmit |
| H2 | **5 seed/import scripts insert-on-miss** using OLD SKUs (`seed_cm_cn_kits`, `seed_old_kits`, `import_wk_history`, `seed_cratejoy_missing_kits`, `fix_orr_items`) | — | quarantine before Phase 2, else they **recreate every deleted duplicate** |
| H3 | `curation_run_items` cascade | 34,002 rows, **~3,678 hit** | ✅ **DECIDED (Hasan, 2026-07-30): accept the loss.** The rows reference items that will no longer exist as separate rows, so they'd be stale anyway. Ting regenerates the report after the merge. **See H3a below.** |
| H3a | Saved reports go **silently incomplete** until regenerated | **26 runs** (2026-05 → 2026-08) each lose ~11% of their item rows | Reports still open and render — they just quietly omit rows, with no warning. So regeneration must happen **promptly after Phase 2**, not eventually. Tell Ting *before* the merge, and flag that the current **2026-08** run is affected too. |
| H4 | `veracore_sync` **last-wins** on shared `veracore_sku` | dict comprehensions | Phase 1 is *not* zero-risk while duplicates exist |
| H5 | Merge is **not atomic** — SELECT→INSERT→DELETE per link, 3,093 deep, webhooks live | — | run in a quiet window; consider a lock |
| H6 | Survivor rule (kit-dominant) **deletes the higher-history row in 5 groups** | 1,835 shipments | switch to **shipment-weighted**, review the 5 |

### 3.1 The 5 groups where the survivor rule is wrong
| Target | v1 would KEEP | v1 would DELETE |
|---|---|---|
| Simon & Schuster nursery book | (HARDCOVER) 437 ships | plain **747 ships** |
| CoverHairCare hold cream | 122 ships | **304 ships** |
| TotesPreggo tote | `+TOTE` 187 ships | `-TOTE` **255 ships** |
| WillowCollective souper spoons | **0 ships** | **203 ships** ← *and it already holds the target SKU* |
| BabyMamaTank (XL) | 115 ships | tshirt 152 + tank(L) 152 |

Four of these five sit in the "AUTO-SAFE — merge unattended" bucket. **Nobody would have reviewed them.**

---

## 4. Sheena's sheet — what still needs her

**A. Wrong cell, correct answer known — sign-off only (10 rows)**
`…CleansingOiol`→`…CleansingOil` · `…(Baby)-32Loads`→drop suffix · `MamaBeanie(KnittedFoldOver…)`→`OBB-MamaBeanie` ·
`CandyLipScrub EXP 05/24`→`EXP05/24` · `+PregnancyTea`→`+Pregnancy Tea` · `…forParentsofMiniHumans`→`…forParents` (×2) ·
`LaborGripSocks(White+Blue)`→`… (White+Blue)` · `BabyBandanaDroolBib`→`BabyBandanaBib` · keep the `PFrame` typo.

**B. Overwrote a correct guess — must confirm (the 2 that matter most)**
- **Row 54** — MotherNoun Maternity Tshirt (XL) answered `OBB-BabyMamaTank(ExtraLarge)`, **byte-identical to row 52 two rows above.** Her own guess `OBB-MotherNounMaternityTshirt(XL)` was correct and is free. Fixing this kills the "tank merged with tshirt" group.
- **Row 143** — Rose Argan Hair Mist answered `OBB-TheAromaShop+OrganicFootCare` = **row 99's answer**. Marked **High** confidence. Fixing this kills the AromaShop group entirely.
  → *Confidence is not a triage filter: the single worst error is High.*

**C. Size families — internally impossible, must re-grade**
BellyBrace rows 6/7/72 (MEDIUM→S, SMALL→XL, EXTRA LARGE→L — each of S/L/XL receives two different source sizes) ·
row 48 corrupted cell (Name says LARGE, SKU says EXTRALARGE) · BabyMamaTank 52/53/67 inconsistent ·
row 150 MaternityTshirt(L)→(S) at High confidence.

**D. Splits — v1 would have destroyed these (3, not 1)**
Row 13 (4 comma-separated SKUs) · **rows 15+16** (same Current SKU, two different answers, her note says "should be entered as different SKUs" — v1's "dedupe by current-sku" would silently discard one) · **row 77** (`[S,M,L,XL,XXL]` answered with a single XXL — no comma, so v1 treats it as an ordinary rename and **collapses five sizes into one**).

**E. Blank** — row 110 `OBB-EVOLUE - HYDRATING SERUM`.

**F. Semantic merges no algorithm can reject — yes/no needed**
AromaShop FootCare←HairMist · lemongrass bar soap←`GOOD2GO VANILLA ALMOND BARS` · two EXP-dated items→live parents ·
AYNIL bracelet variants · `HP+ALLISWELL` vs `HS+ALLISWELL` (different brand codes) · CloudDoorknobSign←SshhSleepingBaby ·
PlushSwaddle(DARKBROWN)→plain · **MamaCap→MamaBeanie** (a cap is not a knitted fold-over beanie — same target, still questionable).
**All are currently in the AUTO-SAFE bucket.**

---

## 5. Phases

### PHASE 0 — prerequisites (all blocking)
1. Sheena answers §4 A–F.
2. Decide H3 (`curation_run_items`: accept the ~3,678-row loss, or repoint).
3. Fix H1 (uppercasing) and H2 (quarantine the 5 seed scripts).
4. Fresh backup (`scripts/backup_obb_tables.py`), counts verified.
5. Re-derive **all** numbers after the sheet is corrected — they will move again.

### PHASE 1 — `veracore_sku` fill
Only for items **not** in a merge group (H4: while twins exist, shared `veracore_sku` routes stock to one arbitrarily).
Reversible (`SET veracore_sku = NULL`).

### PHASE 2 — merge (50 groups / 60 retirements)
Order is mandatory (FK CASCADE):
`INSERT` repointed links **if absent** → `DELETE` old links → **`DELETE` the item LAST**.
Survivor = **shipment-weighted**, with the 5 groups in §3.1 individually confirmed.

### PHASE 3 — rename `sku` on survivors
Only 26 of 50 survivors already hold their target — **22 still need renaming**. Not cosmetic.
Fully dependent on Phase 2 deleting every retiree; **any group Sheena rejects becomes an unhandled `UNIQUE` violation.**

### PHASE 4 — loose ends
The 3 splits (rows 13, 15+16, 77) · the blank · `resolve_history_item_ids` → `.ilike`.

---

## 6. Test requirements (v1's sandbox was invalid)

The v1 sandbox scored `13/13` but **3 assertions passed the literal `True`** and the idempotency test ran on an
empty list. **Real score 9/13.** A valid harness must:
1. Assert on **computed values only** — no literal `True` anywhere.
2. Exercise the **INSERT branch** (v1 reported `moved=0`; the real run inserts **95** times).
3. Cover **3–4 member groups** (8 groups have them) — v1 tested a 2-item toy.
4. Cover the **`UPDATE items SET sku`** path — **zero coverage today**.
5. Cover **partial-failure resume** and carry `quantity` on repoint.
6. Include the pre-flight that would have caught the v1 bug: **no item may appear in more than one merge group.**

---

## 7. Go / No-Go

| # | Gate | State |
|---|---|---|
| 1 | Sheena answered §4 | ❌ |
| 2 | H3 decision recorded | ✅ accept loss; Ting regenerates promptly after Phase 2 (H3a) |
| 3 | H1 + H2 fixed | ✅ `66ba2de` + `83c184b` (H1, incl. the ilike wildcard bug) · `977f2df` (H2 + scheduler guard), all verified |
| 4 | Numbers re-derived post-correction | ❌ |
| 5 | Valid test harness (§6) green | ❌ |
| 6 | Backup taken + verified | ❌ |
| 7 | Quiet window (no cron/webhook overlap) | ❌ |

**No phase runs until all 7 are green.**

---

## 8. Honesty note

v1 claimed "~60 VERIFIED". That count included assertions that could not fail and claims that were
factually wrong about the code. This v2 states only what was reproduced **twice, independently**.
Where something is unproven it is marked ❌ above rather than assumed.
Full audit trail: `tasks/w3r552m9m.output`.
