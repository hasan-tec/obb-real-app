# OBB Curation Engine — Architecture

## System Overview

Oh Baby Boxes receives subscription orders from Shopify and Cratejoy, runs a decision engine to assign the right kit to each customer, then pushes approved orders to VeraCore (warehouse/3PL) for fulfillment.

```
Shopify ──────────────────────────────────────────────┐
  (order webhooks via HMAC)                            │
                                                       ▼
Cratejoy ─────────────────────────────────────────► FastAPI (app.py)
  (order webhooks via HMAC)                       on Heroku 1x dyno
                                                       │
                                                       │  Decision Engine
                                                       │  ↓ calculate trimester
                                                       │  ↓ FIFO kit selection
                                                       │  ↓ size/gender matching
                                                       │
                                                  Supabase (PostgreSQL)
                                                  ├── customers
                                                  ├── decisions
                                                  ├── kits / kit_items
                                                  ├── shipments
                                                  ├── activity_log
                                                  └── veracore_sync_log
                                                       │
                              ┌────────────────────────┤
                              │                        │
                              ▼                        ▼
                         VeraCore                Google Sheets
                    (SOAP AddOrder)              (decision export
                    (REST inventory)              fallback / audit)
                              │
                              ▼
                        Pirate Ship
                      (CSV export for
                       shipping labels)
```

## Components

### FastAPI app (app.py)
Single-file monolith on Heroku (1 web dyno). All routes, business logic, webhook receivers, and the background scheduler live here.

- **Webhook receivers** — `POST /webhooks/shopify/orders/create`, `POST /webhooks/cratejoy/order`. Verify HMAC, extract quiz data (due date, size, gender, daddy), run decision engine, write to Supabase.
- **Decision engine** — Calculates trimester from due date, selects kit via FIFO age rank, stores pending decision.
- **VeraCore push** — On approve, runs `submit_to_veracore()` as a FastAPI `BackgroundTask` (avoids Heroku 30s gateway timeout). Uses SOAP `AddOrder`.
- **APScheduler-style daemon** — Background daemon thread checks hourly. Monthly curation report triggers on the 1st of month at 06:00 UTC. Daily VeraCore inventory sync at 04:00 UTC.

### Supabase (PostgreSQL)
Primary database. Service role key bypasses RLS for all operations. Auth uses Supabase Auth (built-in).

| Table | Purpose |
|-------|---------|
| `customers` | One row per subscriber — email, due_date, clothing_size, trimester, platform |
| `kits` | Available box SKUs — trimester, size_variant, quantity_available, age_rank, is_welcome_kit |
| `kit_items` | Junction: items inside each kit |
| `items` | Inventory item catalog |
| `item_alternatives` | Substitution rules for out-of-stock items |
| `decisions` | One row per curation decision — status, kit_sku, veracore_order_id, veracore_status |
| `shipments` | Historical shipment records (one per box shipped) |
| `shipment_items` | Items in each historical shipment |
| `activity_log` | Audit trail — every approve/reject/retry/error |
| `curation_runs` | Monthly report metadata |
| `curation_report_details` | Line-level curation report rows |
| `committed_items` | Forward planner committed inventory |
| `projection_runs` | Forward planner run metadata |
| `veracore_sync_log` | VeraCore inventory sync run log |
| `app_settings` | Key-value config editable from UI (e.g. veracore_freight_service) |

### VeraCore (3PL Warehouse)
Fulfillment system. Two integrations:
- **SOAP AddOrder** — pushes an approved decision as a warehouse order. Endpoint: `{VERACORE_BASE_URL}/VeraCore.Services.asmx`
- **REST Inventory** — reads `quantity_available` per Offer ID. Endpoint: `{VERACORE_BASE_URL}/inventory`

Env vars: `VERACORE_BASE_URL`, `VERACORE_USER_ID`, `VERACORE_PASSWORD`, `VERACORE_SYSTEM_ID`.

### Google Sheets
Fallback export / audit trail. Each new decision is written to a configured Sheet via service account. On approve/reject, the existing row is updated. Configured via `GOOGLE_SHEET_ID` + `GOOGLE_SERVICE_ACCOUNT_JSON`.

### Pirate Ship
Shipping labels — no API integration. Team exports via `GET /decisions/export-veracore-csv` and uploads the CSV to Pirate Ship manually.

## Deployment

- **Platform:** Heroku (single web dyno)
- **Runtime:** Python 3.11, Uvicorn ASGI server
- **Config:** All secrets in Heroku config vars (env vars)
- **Logs:** `heroku logs --tail` or Papertrail addon

## Auth

Supabase Auth (email+password, no OAuth). Two roles set in user metadata:
- `admin` — full read+write access
- `viewer` — read-only, no approve/reject/edit

FastAPI `AuthMiddleware` validates the JWT cookie on every request except `/login`, `/health`, and incoming webhook POSTs from Shopify/Cratejoy.
