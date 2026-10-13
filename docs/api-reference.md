# OBB Curation Engine — Route / API Reference

Every route in `app.py`, current as of 2026-10-06. The line numbers are approximate; search for the path.

**Auth:**
- **Public** means no login is needed.
- Every other route needs the login cookie.
- Every POST needs the `admin` role (viewers get 403).
- Pages return HTML. `JSON` marks routes that return JSON. "Redirect" means a form post that redirects back with a `msg=` banner.

## Inbound webhooks (Public — called by Shopify / Cratejoy)

| Method | Path | Notes |
|---|---|---|
| POST | `/webhooks/shopify/orders/create` | New order → decision. HMAC `X-Shopify-Hmac-Sha256` (secret `SHOPIFY_WEBHOOK_SECRET`). Duplicate event ids are ignored. |
| POST | `/webhooks/shopify/orders/updated` | Ship-to sync and cancellations. |
| POST | `/webhooks/shopify/customers/update` | Customer detail sync. |
| POST | `/webhooks/cratejoy/order` | Legacy Cratejoy receiver (subscription and order events). Cratejoy boxes are now created by the daily sync. |
| POST | `/api/cratejoy/register-webhooks` | Registers the Cratejoy webhooks above with `BASE_URL`. Idempotent. |

All webhooks return 200 even when processing fails, so the platform doesn't retry-storm. Failures show on the **Webhooks** page with status `failed`, and can be replayed.

## Scheduled jobs, manual triggers (JSON)

| Method | Path | Runs |
|---|---|---|
| POST | `/api/cratejoy/daily-sync` | Cratejoy due-box sync |
| POST | `/api/cratejoy/daily-reconcile` | Cratejoy cancellation, ship-to and status reconcile |
| POST | `/api/shopify/hold-reconcile` | Shopify on-hold pause/resume |
| POST | `/veracore/sync-inventory-now` | VeraCore inventory sync (Redirect) |
| POST | `/veracore/sync-expiry-now` | VeraCore expiry sync (Redirect) |

## Auth and system

| Method | Path | Notes |
|---|---|---|
| GET / POST | `/login` | Public. Supabase Auth email + password. Sets `obb_access_token` / `obb_refresh_token` cookies. |
| GET | `/logout` | Clears cookies. |
| GET | `/health` | Public. Liveness check (JSON). |
| GET | `/` | Dashboard: counts, recent activity, VeraCore status. |
| GET | `/activity` | Full activity log. |
| GET | `/settings` | Settings page. |
| GET | `/flow-diagram` | Visual flow of the engine. |
| POST | `/api/recalculate-all-trimesters` | Recompute every customer's trimester now. |
| POST | `/api/backfill-age-ranks` | Recompute `kits.age_rank` from SKUs. |
| POST | `/api/fix-gsheet-headers` | Repair the Google Sheet header row. |
| POST | `/api/test-webhook`, `/api/test-webhook-cratejoy` | Admin test harness: runs a sample payload through the engine. **Writes real rows**, so use it with care. |

## Webhook log

| Method | Path | Notes |
|---|---|---|
| GET | `/webhooks` | Inbound webhook list with filters. |
| GET | `/webhooks/{id}` | Raw payload and processing result. |
| POST | `/webhooks/{id}/replay` | Re-process a logged webhook. Guarded: no duplicate box (see business-logic §1). |

## Decisions (the main work page)

| Method | Path | Notes |
|---|---|---|
| GET | `/decisions` | Filters: `status`, `type` (decision type), `platform`, `trimester`, `size`, `order_type`, `month`, `q` (search), `shipped_on` + `tzo` (the viewer's timezone offset in minutes), `sort` + `dir`, paging. Also `bulk_job` (shows the progress banner). |
| POST | `/decisions/{id}/approve` | Approve. Stock −1. VeraCore push if enabled. |
| POST | `/decisions/{id}/reject` | Reject. |
| POST | `/decisions/{id}/ship` | Mark shipped, set `shipped_at`, write shipment history. |
| POST | `/decisions/{id}/veracore-retry` | Retry a failed VeraCore push. |
| POST | `/decisions/bulk-action` | Form: `action` = approve, ship, reject or recurate; `decision_ids` (repeated); `redirect_qs`. Starts a background job and redirects to `/decisions?…&bulk_job={id}`. |
| GET | `/decisions/bulk-job/{job_id}` | JSON progress `{status, done, total, …}`. Kept in memory, lost on restart. |
| POST | `/decisions/upload-tracking` | Multipart `file` = Pirate Ship "Export Tracking Data" (.xlsx/.csv). JSON `{summary, details}`. Pushes tracking to Shopify or Cratejoy. |
| GET | `/decisions/export-csv` | Pirate Ship CSV of the current filter. Defaults to `approved`, or to `shipped` when `shipped_on` is set. |
| POST | `/decisions/export-csv-selected` | Pirate Ship CSV of the checked rows. |
| GET | `/decisions/export-veracore-csv` | VeraCore import-format CSV (the fallback when the API push isn't used). |
| POST | `/decisions/export-sheet` | Push the current filter to the Google Sheet. |

## Customers

| Method | Path | Notes |
|---|---|---|
| GET | `/customers` | List with filters. |
| GET | `/customers/export-csv` | CSV of the list. |
| GET | `/customers/{id}` | Detail: profile, history, decisions, rejected kits, override. |
| GET | `/customers/{id}/export-pirateship` | One-customer Pirate Ship CSV. |
| POST | `/customers/add` | Create a customer (and run the engine). |
| POST | `/customers/{id}/edit` | Edit due date, size, address, status, etc. |
| POST | `/customers/{id}/remove` | Delete a customer. |
| POST | `/customers/{id}/recurate` | Re-run the engine (blocked while a pending box exists). |
| GET | `/customers/{id}/override-kits-json` | JSON list of the kits this customer is eligible for. |
| POST | `/customers/{id}/override-kit` | Assign a chosen kit (`manual-override`). |
| POST | `/customers/{id}/shipments/add` | Add a history row by hand. |
| POST | `/customers/{id}/shipments/{sid}/edit` | Edit a history row. |
| POST | `/customers/{id}/shipments/{sid}/remove` | Remove a history row. |

## Kits, items, alternatives

| Method | Path | Notes |
|---|---|---|
| GET | `/kits`, `/kits/{id}` | List, and detail with item contents. |
| POST | `/kits/add`, `/kits/{id}/edit`, `/kits/{id}/remove` | Kit CRUD (SKU, trimester, stock, welcome/universal flags, size variant, VeraCore SKU). |
| POST | `/kits/{id}/items/add`, `/kits/{id}/items/quick-add`, `/kits/{id}/items/{item_id}/remove` | Kit contents. |
| GET | `/items` | Item catalog. |
| POST | `/items/add`, `/items/{id}/edit`, `/items/{id}/remove` | Item CRUD. |
| GET | `/item-alternatives` | Items treated as "the same thing" for repeat blocking. |
| POST | `/item-alternatives/add`, `/item-alternatives/remove` | Manage the pairs. |

## Curation report and forward planner

| Method | Path | Notes |
|---|---|---|
| GET | `/curation-report` | Past runs. |
| POST | `/curation-report/generate` | Start a run (background job) → `/curation-report/job/{job_id}`. |
| GET | `/curation-report/job/{job_id}`, `…/status` | Job page and JSON status. |
| GET | `/curation-report/{run_id}` | View a run. |
| POST | `/curation-report/{run_id}/export-sheet`, `/curation-report/{run_id}/delete` | Export or delete a run. |
| GET | `/forward-planner` | Projections. |
| POST | `/forward-planner/generate` | Start a projection → `/forward-planner/job/{job_id}` (+ `/status`). |
| POST | `/forward-planner/{run_id}/delete`, `/forward-planner/commit-items`, `/forward-planner/clear-committed` | Manage runs and committed stock. |

## VeraCore

| Method | Path | Notes |
|---|---|---|
| GET | `/veracore` | Status, sync history, push failures, settings. |
| POST | `/veracore/save-settings` | Freight service and other settings (`app_settings`). |
