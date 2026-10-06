# OBB Curation Engine — Architecture

Current as of 2026-10-06 (main `e11f647`, migrations 001–024).

## What the system does

Oh Baby Boxes sells pregnancy and postpartum subscription boxes on Shopify and Cratejoy. For every box that is due, the engine picks the right kit, based on the customer's trimester at ship time, clothing size, items already received, and stock. Staff then review, approve or ship it, export it to Pirate Ship for labels, and upload the tracking file. Tracking is then pushed back to Shopify or Cratejoy.

```
 Shopify ── webhooks (orders/create, orders/updated, customers/update) ──┐
                                                                          ▼
 Cratejoy ── daily pull of the unshipped-shipment queue (07:00 UTC) ──► FastAPI app (app.py)
             (+ legacy webhook receiver)                                Heroku, 1 web dyno
                                                                          │
                                         decision engine (assign_kit) ◄───┤
                                                                          ▼
                                                              Supabase (PostgreSQL + Auth)
                                                                          │
              ┌───────────────────────────────┬───────────────────────────┼─────────────────────┐
              ▼                               ▼                           ▼                     ▼
      Decisions page (staff)          Pirate Ship CSV export        Google Sheet export     VeraCore (3PL)
   approve / reject / ship /          → labels bought in             (audit / batch view)   inventory + expiry sync,
   override / recurate / bulk           Pirate Ship                                         order push (when enabled)
              │
              ▼
   Upload Pirate Ship tracking file → Shopify fulfillment / Cratejoy shipment marked shipped
```

## Components

| Component | Where | Notes |
|---|---|---|
| Web app | `app.py` (~11k lines, FastAPI + Jinja2) | All routes, webhook receivers, decision engine, bulk actions, exports, scheduler. |
| Curation report | `curation_report.py` | Monthly forward look at every active customer (runs on the 3rd, or on demand). |
| Forward planner | `projection_engine.py` | Projects kit/item demand for the next N months; staff can commit stock. |
| Shopify client | `shopify_client.py` | Outbound Admin API: fulfillments, hold status, order lookups. |
| Cratejoy client | `cratejoy_client.py` | Outbound API: shipments, subscriptions, customers. |
| VeraCore | `veracore_client.py`, `veracore_sync.py` | SOAP AddOrder push, REST inventory + expiry sync, shipment poll (poll currently disabled). |
| UI | `templates/*.html` | Server-rendered pages. Dark theme. Small vanilla JS on each page. |
| Schema | `migrations/001…024` | Plain SQL, run by hand in the Supabase SQL editor, in order. |
| One-off tools | `scripts/` | Imports, backfills, repairs. All dry-run by default, `--apply` to write. |
| Tests | `tests/` | pytest. Fake DB, no network. |

## Runtime model

- **One process.** Procfile: `web: uvicorn app:app --host 0.0.0.0 --port $PORT`. Single worker. In-memory state (bulk-job progress, report job status) lives in this process and resets on a dyno restart.
- **Background scheduler.** A daemon thread started at import time (`_start_scheduler`). It wakes every hour and runs each daily job once. A `/tmp` lock file per job/day (`_schedule_lock`) stops a job running twice. Set `OBB_DISABLE_SCHEDULER=1` to switch the scheduler off. Do this in every script and test, or importing `app` will start production jobs.

  | Job | When (UTC) | Function |
  |---|---|---|
  | VeraCore inventory + expiry sync | 04:00 hour (only if VeraCore creds are set) | `veracore_sync.run_inventory_sync`, `run_expiry_sync` |
  | Trimester refresh | after 06:00 daily | `_refresh_customer_trimesters` |
  | Monthly curation report | the 3rd, after 06:00 | `curation_report.run_monthly_report` (ship date = 14th) |
  | Cratejoy daily sync (creates decisions for due boxes) | after 07:00 daily | `_cratejoy_daily_sync` |
  | Cratejoy reconcile (cancellations, ship-to, status) | after 07:00 daily | `cratejoy_daily_reconcile` |
  | Shopify on-hold reconcile (pause/resume) | after 07:00 daily | `shopify_daily_reconcile` |
  | VeraCore shipment poll | **disabled** (not in SOW) | `veracore_sync.run_shipment_poll` |

  The same jobs can be run manually: `POST /api/cratejoy/daily-sync`, `/api/cratejoy/daily-reconcile`, `/api/shopify/hold-reconcile`, `/veracore/sync-inventory-now`, `/veracore/sync-expiry-now`.
- **Bulk actions** (approve / ship / reject / recurate on many boxes) run on their own thread. The page shows a progress banner that polls `GET /decisions/bulk-job/{id}`. The request returns at once, so Heroku's 30-second router timeout is never hit.
- **Database clients.** `get_supabase()` gives every non-main thread its own Supabase client. The supabase-py HTTP/2 connection is not safe to share across threads: sharing it caused the 2026-10-01 `[Errno 11]` / `ConnectionTerminated` failures. Never create a client directly. Always call `get_supabase()`.

## Data model (main tables)

| Table | Holds |
|---|---|
| `customers` | One row per **recipient profile** (email + recipient). Due date, trimester, size, platform, subscription status, platform ids. One email can have several profiles (gift senders, migration 022). |
| `decisions` | One row per box to send. Statuses: `pending`, `approved`, `shipped`, `rejected`. Also stores the kit, order/shipment ids, `decision_type`, `reason`, ship-to and billing address, tracking, VeraCore fields, `order_type`, `shipped_at` (024). |
| `shipments` / `shipment_items` | What each customer actually received. The engine uses this history to block repeat kits and items. |
| `kits` / `kit_items` / `items` / `item_alternatives` | Catalog. `kits.quantity_available` = units on hand. `age_rank` = FIFO order. `is_welcome_kit`, `is_universal`, `size_variant`, `build_month`. |
| `webhook_logs` | Every inbound webhook: raw payload, status, error. Can be replayed from the Webhooks page. |
| `activity_log` | What the dashboard "Activity" feed shows (approvals, bulk summaries, exports, sync results, alerts). |
| `curation_runs` (+ `curation_run_customers`, `curation_run_items`, `curation_committed_items`) | Monthly curation report runs. |
| `projection_runs` | Forward planner runs. |
| `veracore_sync_log`, `kit_stock_alerts` | VeraCore sync history, low-stock alerts. |
| `app_settings` | Key/value settings editable in the UI (e.g. VeraCore freight service). |

## Auth

Supabase Auth, email + password. The role is in user metadata: `{"role": "admin"}` or `{"role": "viewer"}`; with no role set, the user is a viewer. `AuthMiddleware` checks the cookie on every request. Viewers get a 403 on any POST. These paths need no login: `/login`, `/logout`, `/health`, `/webhooks/shopify*`, `/webhooks/cratejoy*`, `/api/cratejoy/register-webhooks`. How to add users: [auth-setup.md](auth-setup.md).

## Hosting

- Heroku app `obb-real-d4e16a8bb2ff`. URL `https://obb-real-d4e16a8bb2ff.herokuapp.com`. Auto-deploys from GitHub `main`. Python version is set in `.python-version`.
- All secrets are Heroku config vars. See the list in [../HANDOVER.md](../HANDOVER.md#environment-variables).
- Supabase project `obb` (ref `tkcvvjxmzfjaesdhyfiy`).
- Logs: `heroku logs --tail -a obb-real-d4e16a8bb2ff`. App-level history is also on the Webhooks and Activity pages.
