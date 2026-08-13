# Dynamic Customer Sync — Fix Plan

**Status:** v2 — audited (adversarial, read-only, live-DB-verified). Corrections folded in below.
Ready to implement.
**Trigger:** `imspcl2@yahoo.com` — Shopify screenshots (Recharge orders Apr–Nov 2025, name "Beka
Taylor") and Cratejoy screenshots (9 shipments Nov 2025–Jul 2026, recipient "Brianna Lipscomb",
same address) prove she switched from Shopify to Cratejoy in Nov 2025. Our DB still shows
`platform=shopify`, `due_date=2025-09-25` (her last Shopify value), 9 months stale.

---

## 1. What's actually broken (evidence, not guesses)

### 1.1 Shopify — mostly correct, two defects that matter to this plan
`shopify_order_webhook` (`app.py:2258-2301`) refreshes `due_date` / `clothing_size` / `baby_gender`
present-only on every subscription order — first order or renewal alike — so those three fields
self-refresh correctly each cycle. **Two exceptions, both load-bearing for this plan:**

- **`platform` is NOT set unconditionally to `"shopify"`.** `app.py:2284-2287` sets it to `"both"`
  whenever the existing row already has a `cratejoy_customer_id`. The identical pattern exists at
  `app.py:3712-3715` (webhook replay), `2890-2893` (Cratejoy webhook), `3915` (Cratejoy replay), and
  `6708-6709` (test endpoint). This directly interacts with §2.2 step 3 below — see §2.1.
- **`wants_daddy_item` is written unconditionally** (`app.py:2280`) from a parser that defaults it to
  `False` (`app.py:1396`), so a renewal order lacking the second-parent attribute silently resets it.
  Pre-existing, out of scope here, noted so this path isn't mistaken for a fully clean model — the
  fix for it already exists elsewhere: `orders/updated`'s `_daddy_present()` probe at
  `app.py:2579-2589`, which §2.2 must copy.

### 1.2 Cratejoy — the real bug
`process_cratejoy_box` (`app.py:614-723`) is called once per unshipped box by the daily sync. For a
**brand-new** customer (`cust_row is None`, line 645) it calls `_cj_enrich_new_customer` to pull
survey + address from the Cratejoy API, sets `platform="cratejoy"`, and inserts.

For an **existing** customer (`cust_row` found — by `cratejoy_customer_id` first, then by email,
lines 636-643) it does **nothing to the `customers` row**. Line 666 goes straight to
`cust_id = cust_row["id"]` and on to decision creation. No survey refresh, no `platform` write, no
backfill of `cratejoy_customer_id` when the match was by email only. **This is the entire bug.**

`cratejoy_order_webhook` (the live webhook, currently unregistered — see §1.3) does **not** have this
bug: its existing-customer branch already refreshes both the customer record (`app.py:2907`) and
survey/address (`app.py:3128-3130`). Its silence in this plan is correct on the merits, not an
oversight — see §1.3 for why it still needs one of this plan's changes anyway.

### 1.3 Measured blast radius (live DB, read-only) — re-derived independently by audit, confirmed

```
customers.platform:  {shopify: 2333, cratejoy: 154}     both: 0 rows, ever
decisions.platform:  {shopify: 2045, cratejoy: 278, null: 8}

customers whose LATEST decision.platform disagrees with customers.platform: 2
   bwagner95@comcast.net   customers.platform=shopify  latest decision.platform=cratejoy
   imspcl2@yahoo.com       customers.platform=shopify  latest decision.platform=cratejoy
   (each has exactly ONE decision ever, and it is a Cratejoy one)

customers with decisions on BOTH platforms ever: 0
customers with decisions on different platforms in the SAME month: 0  (stricter test, also 0)
customers with BOTH shopify_customer_id AND cratejoy_customer_id set: 0

cratejoy customers with >1 decision: 104 of the 138 with any decision (154 Cratejoy customers total)
```

**Evidentiary limits of the above — do not over-trust these zeros.** The `decisions` table's earliest
row is **2026-03-26** (full range 2026-03-26 → 2026-08-12, 2,331 rows). There is no 2025 history, so
"decisions on both platforms ever: 0" really means "in the last 4.5 months" and **structurally cannot
observe `imspcl2@yahoo.com`'s own Apr–Nov 2025 Shopify era** — the exact switch that motivated this
plan predates the table it's being measured against. Separately, **`shopify_customer_id` is NULL on
1,811 of 2,487 rows (73%)** — bulk-imported customers never received one — so "0 rows with both IDs
set" is partly an artifact of import history, not proof the state can't occur. Treating simultaneous
dual-platform activity as out of scope is an accepted product assumption below, not a data-proven
fact.

**Liveness of other Cratejoy write paths, checked because they'd need the same fix if reachable:**
`webhook_logs` shows **38 Cratejoy events total, none since 2026-06-24** — consistent with
`CRATEJOY_DAILY_SYNC_PLAN.md` ("5 hooks unregistered"). But `/webhooks/cratejoy/order` is still
mounted with no HMAC check, and `/api/cratejoy/register-webhooks` can re-arm all 5 in one call — so
it's **dormant, not dead**, and still carries the `'both'` branch this plan removes (§2.1).
`_cratejoy_monthly_sweep` is confirmed fully dead — defined at `app.py:397`, zero callers anywhere.
`replay_webhook` (`app.py:3561`, Shopify branch `3611-3791`) is fully live; measured usage is **3
`replay_orders/create` and 6 Cratejoy replays, ever** — its Shopify branch carries the same `'both'`
logic at `3712-3715`.

---

## 2. Design

### 2.1 Platform = last platform to author a real *subscription* event. Never machine-assign "both".

`platform` becomes "whichever platform's subscription webhook/sync most recently created or renewed
this customer's decision." Four parts, all of which must ship together — shipping only part of this
makes things worse, not better (see the C-1 warning below):

1. **Remove the `'both'` branch from every automated writer:** `app.py:2284-2287`, `3712-3715`,
   `2890-2893`, `3915`, `6708-6709`. Each writer sets its own platform unconditionally instead.
   **Non-negotiable together with §2.2 step 3.** §2.2 step 3 backfills `cratejoy_customer_id`, which
   is exactly the precondition `app.py:2284` tests for (`if existing.get("cratejoy_customer_id")`).
   Shipping the backfill without removing the `'both'` branches means the *next* Shopify order for
   that same customer flips her to `platform="both"` — creating, for the first time in the system's
   history, the exact dual state this plan exists to keep out. **These two changes ship in the same
   commit.**
2. **Only a subscription event may author `platform`.** Remove `"platform"` from the non-subscription
   `address_only` field whitelist at `app.py:2290` and `app.py:3718`. Today, a Cratejoy subscriber who
   buys one unrelated retail item from the Shopify store flips her platform back to `"shopify"` on a
   plain non-subscription order — silently dropping her out of `_cj_reconcile_customer_statuses`
   (`app.py:878`, filters `platform in (cratejoy, both)`). This is a more likely and more dangerous
   failure mode than the replay scenario in §2.4, and this one-token fix closes it directly.
3. **`'both'` stays a legal value, it is simply never machine-assigned.** No migration —
   `migrations/001_initial_schema.sql:23`'s CHECK constraint permits `'both'` as one of three allowed
   values; ceasing to *assign* it needs no schema change. No UI change — `edit_customer`
   (`app.py:5799`) and `templates/customer_detail.html`'s manual dropdown keep the option (backend
   task; templates are out of scope), and every reader that filters on `'both'`
   (`app.py:431/878/1041/3446-3447`) keeps working unchanged for the handful of rows staff set by
   hand.
4. **Log every change.** `logger.warning("[PLATFORM] %s: %s -> %s (source=%s)", email, old, new,
   path)` whenever a write actually *changes* `customers.platform`. Flips are rare — 2 of 2,487 rows
   today — so each one becomes individually visible and auditable rather than silent.

### 2.2 Fix `process_cratejoy_box`'s existing-customer branch

**Insertion point matters and was wrong in draft v1.** Insert the refresh **after** the
`history_pending` guard (`app.py:673-674`) and **before** the month guard (`app.py:676-689`) — and
**inside the function's `dry_run` check**. The existing-customer `dry_run` early-return is at
`app.py:691-694`, *downstream* of the naive line-666 insertion point from v1; a refresh written there
would run real writes during a "preview" and corrupt data the first time §5's verification dry-run is
executed.

A **paused** customer (`app.py:669-670`) still gets refreshed — platform and survey data are facts
about the customer, independent of whether she's currently being decisioned, and a paused customer
with a stale platform is exactly the case invisible to `_cj_reconcile_customer_statuses`. A
**`history_pending`** customer does not get refreshed — quarantined by design (only 1 such row
exists today).

1. **Survey/address refresh with an explicit per-field policy.** A blanket `if val:` merge (v1's
   plan) is unsafe — `_cj_enrich_new_customer` has three traps that only matter once the same code
   runs against an *existing* row instead of a fresh insert:
   - **`country`** (`app.py:580`) is returned unconditionally, defaulting to `"US"`, even when the
     address fetch throws or 404s (the exception is swallowed at 569-570). `"US"` is truthy, so a
     naive merge silently downgrades a real non-US customer. **43 live rows currently have a Canada
     address.** → **Exclude `country` from the refresh entirely.**
   - **`baby_gender`** (`app.py:606`) returns the truthy literal `"unknown"` for any survey answer
     that doesn't contain "boy" or "girl". → **Write only when the fetched value is `"boy"` or
     `"girl"`.**
   - **`wants_daddy_item`** (`app.py:608`) returns `False`, which is falsy — a truthy merge silently
     drops a legitimate yes→no change. → **Presence-probe the second-parent survey question the way
     `_daddy_present()` (`app.py:2579-2589`) already does for the Shopify side, and write the boolean
     only when that question was actually answered.**
   - **Never refresh `email`, `first_name`, or `last_name`** from the Cratejoy payload. 20% of
     decisions (462 of 2,331) have a `ship_to` name differing from the customer row's stored name —
     the Cratejoy name is not reliably this row's identity (see step 3).
   - Do not re-derive `trimester` here — `_cj_enrich_new_customer` computes it from `date.today()`
     (`app.py:599`), not the box's own `ship_date`; the daily trimester-refresh job (`app.py:1105`)
     already owns this field correctly.
2. **Set `platform = "cratejoy"` unconditionally** — safe only because §2.1 step 1 removes the `'both'`
   branches in the same change.
3. **Backfill `cratejoy_customer_id` only under a uniqueness guard.** Write it only when the stored
   value is `NULL` **and** no other `customers` row already holds that `cj_cust_id`. If the
   email-matched row's stored name disagrees with the Cratejoy customer's name, **log a WARNING and
   do not write** — don't guess. Measured justification: `owens1111@yahoo.com` and `jcctj4@gmail.com`
   already each serve **two distinct real Cratejoy recipients under one email row**, and the trigger
   case is itself one of these — the `imspcl2@yahoo.com` row is "Beka Taylor" while its live Cratejoy
   decision ships to "Brianna Lipscomb". Because the lookup tries `cratejoy_customer_id` before email
   (`app.py:638-640`), a wrong backfill would permanently misroute a *different* subscriber's future
   syncs onto this row. **Open question for Hasan, not guessed here: is Brianna Lipscomb the same
   person as Beka Taylor under a name change, or a different recipient sharing the address/email?**

**Rate/volume — corrected, v1's claim was wrong.** This does not fire once per customer per month.
It fires **once per undecisioned due box** — line 666 sits upstream of the month guard, and that
guard doesn't even cap decisions (`.neq("status","rejected")` — 24 customer-months already carry >1
Cratejoy decision live, 12 of them >1 *non-rejected*). Real cost: **89 active/cancelled-prepaid
Cratejoy customers** × up to 2 Cratejoy API calls each (subscription + survey) ≈ **~180 extra calls a
month that don't happen today**. Affordable, but it's a genuine increase, not "no increase" as v1
claimed. A box that already has a decision short-circuits before any of this (`app.py:633-634`), and
a box outside the sync window never reaches the function at all (`app.py:755`).

### 2.3 One-time backfill — DB-only, no bulk API calls

Fix the 2 customers measured as mismatched (§1.3) by setting `customers.platform` from their own
latest `decisions.platform`. Pure DB read + write, no external API calls, no rate-limit exposure.

Both also carry a live decision curated from stale data — `imspcl2@yahoo.com` has a 2026-07-01
`pending`/`needs-curation` decision (`kit_sku=None`, from the stale `due_date=2025-09-25`). No wrong
kit is currently queued, but it sits parked. **After the backfill, re-curate both customers** (or
explicitly leave them `needs-curation` and say so).

**Deliberately not doing a bulk survey-data backfill** for the other 104 exposed Cratejoy customers —
they self-heal via §2.2 within 30 days, since the daily sync creates a decision every month per
active customer. Hammering the Cratejoy API for 104 customers at once to fix data that fixes itself
within a month is the speculative work the guidelines say to skip.

### 2.4 Event-ordering / replay safety — RESOLVED: do nothing extra, plus one companion change

**Decision: no `platform_source_at` timestamp column.** Reasoning:
1. It wouldn't cover the dominant real failure mode. The likeliest wrong flip is a *live,
   current-dated, non-subscription* Shopify retail order (§2.1 step 2) — its event date genuinely
   *is* the newest, so an "is this older than what's stored?" comparison would let it through anyway.
2. The scenario a timestamp column *would* catch — a replayed old webhook — is measured at **3
   lifetime `replay_orders/create` events**, operator-initiated, single-record, deliberate.
3. §2.1 step 2 (drop `platform` from the non-sub whitelist) plus step 4 (WARNING on every change)
   cover more real risk for one removed token and one log line, with no schema change and no new
   state to keep consistent.

---

## 3. Exact code changes

1. `app.py` — `process_cratejoy_box`: refresh block per §2.2, inserted after the `history_pending`
   guard (673-674), before the month guard (676-689), **inside the `dry_run` check**.
2. `app.py` — remove the `'both'` branch from all five automated writers: `2284-2287`, `3712-3715`,
   `2890-2893`, `3915`, `6708-6709`. Ships in the same change as #1.
3. `app.py` — remove `"platform"` from the non-subscription `address_only` whitelists at `2290` and
   `3718`.
4. `app.py` — `logger.warning(...)` on any write that changes `customers.platform`.
5. Generalize `_cj_enrich_new_customer`'s API-fetch into a shared helper usable by both branches, with
   the per-field merge policy from §2.2 applied only on the existing-customer path. **Do not change
   new-customer insert behavior** — `country` defaulting to `"US"` is correct for a fresh insert, only
   wrong on top of an existing real address.
6. One-time backfill script (DB-only, §2.3) + re-curate the 2 affected customers.
7. **No migration.**

---

## 4. Non-goals
- Supporting a genuine simultaneous-both-platforms customer — never observed in the measurable
  window, not designed for. `'both'` remains a legal, staff-settable value; it is simply never
  machine-assigned by this plan's code paths.
- Forbidding `'both'` at the schema or UI level.
- Bulk-refreshing survey data for all 104 exposed Cratejoy customers via live API calls — self-heals
  within 30 days via §2.2.
- Fixing the pre-existing unconditional `wants_daddy_item` write on the Shopify `orders/create` path
  (`app.py:2280`) — real, separate, not part of this plan.
- Building `platform_source_at` / replay-safety infrastructure — resolved against in §2.4.

---

## 5. Sequence
1. ~~Audit this plan~~ **DONE** — adversarial, read-only, live-DB-verified; findings folded into v2.
2. Implement §2.1 (remove `'both'` branches + whitelist fix + change-log) and §2.2 (refresh block,
   correctly gated by `dry_run`, with the per-field merge policy) **together, in one change**.
3. Run the 2-customer backfill from §2.3; re-curate both.
4. Verify:
   - Re-run the §1.3 mismatch query → expect 0.
   - **Confirm by code review that the refresh sits inside the `dry_run` guard BEFORE running any
     dry run** — this was the exact mistake in draft v1.
   - Dry-run `/api/cratejoy/daily-sync?dry_run=true` and confirm the existing-customer branch *logs*
     a would-refresh, writes nothing.
   - Confirm `platform='both'` count is still 0 (or matches only pre-existing manual staff sets, if
     any exist by then).
5. Report back — do not push/deploy without explicit confirmation (Heroku is separate from this repo
   until pushed).
