# OBB Curation Engine — Operations Runbook

For whoever maintains the engine after handover.

## Deploy

- `main` auto-deploys to Heroku through the GitHub integration (Heroku → app → Deploy tab).
- Normal flow: branch → PR → merge to `main` → Heroku builds and restarts. A restart:
  - clears the in-memory bulk-job and report-job progress (the work itself is already committed row by row);
  - lets the scheduler run each daily job once again on the new dyno, if that job's hour has passed.
- Roll back: Heroku → Activity → **Roll back** to an earlier release, or revert the merge on GitHub.

## Database migrations

- `migrations/NNN_*.sql`. Run each one by hand in the Supabase SQL editor, **in number order**, **before** deploying code that needs it.
- 001–024 are all applied in production.
- They are written to be re-runnable (`IF NOT EXISTS`).
- Two files share the number 003 and two share 012. Both of each pair are applied. 014/015 were never used.

## Local development

```bash
python -m venv .venv
.venv/Scripts/pip install -r requirements.txt        # Windows; use .venv/bin on macOS/Linux
```
Create a `.env` with the variables in [../HANDOVER.md](../HANDOVER.md#environment-variables) (python-dotenv loads it).

- **Always set `OBB_DISABLE_SCHEDULER=1` locally**, otherwise the daily jobs run against whatever database `.env` points at.
- To test locally against production data, leave the Shopify, Cratejoy, VeraCore and Google variables **blank**. All outbound calls are then skipped.

```bash
OBB_DISABLE_SCHEDULER=1 uvicorn app:app --reload --port 8000
```

## Tests

```bash
OBB_DISABLE_SCHEDULER=1 .venv/Scripts/python -m pytest tests/ -q -p no:warnings
```
The core suites (engine, kit reservation, bulk background jobs, replay guard, shipped-on filter, tracking upload, recipient profiles, ship guard, Shopify cancellation, manual-shipment import) use a fake DB and need no network.

Some older files hit a live server or database and are skipped or fail without one:
- `test_api.py`
- `test_perf.py`
- `verify_*.py`

## Scripts (`scripts/`)

Every script that writes **dry-runs by default** and needs `--apply` to change data. Read the docstring at the top of each one before running it.

| Script | Use |
|---|---|
| `backup_obb_tables.py` | Snapshot or restore the catalog and history tables before a risky change. |
| `cleanup_stale_pending_decisions.py` | Reject old duplicate pending boxes, keeping the newest per customer. |
| `import_manual_shipments.py` / `import_manual_shipments_cj.py` | Record boxes staff shipped outside the engine (Shopify sheet / Cratejoy workbook), so curation sees the history. |
| `import_august_manual_assignments.py`, `import_history.py`, `import_cratejoy_history.py`, `import_wk_history.py` | Older history imports (kept for reference). |
| `add_cratejoy_recipient_profile.py`, `split_recipient_profiles.py`, `backfill_recipient_refs.py` | Recipient-profile tools (one email, several recipients). |
| `backfill_shopify_cancellations.py` | Apply Shopify cancellations to past orders. |
| `backfill_shipped_at.py` | Fill `decisions.shipped_at` for boxes shipped before migration 024 (done 2026-10-03). |
| `diag_*.py`, `audit_data.py`, `sanity_check.py`, `cross_verify.py`, `mega_verify.py` | Read-only diagnostics. |
| `fix_*.py`, `repair_*.py`, `seed_*.py` | One-off fixes already applied. Kept for the record; don't re-run without reading. |

## Daily checks (5 minutes)

1. **Webhooks** page: filter `failed`. Any failed `orders/create` with no box → **Replay** (the guard blocks duplicates).
2. **Activity**: look for the morning sync results (Cratejoy sync and reconcile, Shopify hold reconcile) and any red errors.
3. **Decisions** → `needs-curation`: usually means a kit is out of stock or there are no kits for a trimester. Add kits or stock, then bulk **Recurate**.

## Month start (October onward)

1. Add the new month's kits, with their contents and stock, on the Kits page **before** the 1st. Otherwise new boxes come in as `needs-curation`.
2. After the kits are in, select the kitless pending boxes on Decisions → bulk **Recurate**.
3. The monthly curation report generates itself on the 3rd. Review it for shortfalls.

## When something breaks

| Symptom | Look at |
|---|---|
| A new order has no box | Webhooks page (failed?) → Replay. Check Heroku logs at that time. |
| Box assigned a kit that's out of stock | Kits page stock, and how many pending boxes hold that kit (`kit_reserved_counts`). |
| Tracking upload says `ambiguous` / `needs_review` | That row matched more than one box, or none. Fix that one by hand in Shopify or Cratejoy. |
| Bulk action banner stuck | The dyno restarted mid-job. Reload the page, then rerun the action on the boxes still pending (finished ones are skipped). |
| Login loop | Supabase Auth user exists? Role metadata set? |
| `[Errno 11]` / `ConnectionTerminated` in logs | Some code created or shared a Supabase client across threads. Always use `get_supabase()`. |

Logs: `heroku logs --tail -a <app>`. Every log line is tagged by area: `[SHOPIFY WEBHOOK]`, `[CJ DAILY]`, `[BULK ACTION]`, `[DECISION ENGINE]`, `[SCHEDULER]`, `[UPLOAD TRACKING]`, and so on.
