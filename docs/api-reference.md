# OBB Curation Engine — API Reference

All UI routes return HTML. All routes (except `/login`, `/health`, and webhook receivers) require a valid session cookie set by `/login`.

**Admin** = requires role `admin` in Supabase user metadata. POSTing as `viewer` returns 403.

---

## Auth

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/login` | Public | Login form |
| POST | `/login` | Public | Submit credentials → sets `obb_access_token` + `obb_refresh_token` cookies |
| GET | `/logout` | Public | Clears cookies, redirects to `/login` |

---

## Dashboard

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/` | Any | Dashboard — customer counts, decision stats, VeraCore status |

---

## Webhooks

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/webhooks` | Any | List received webhooks |
| GET | `/webhooks/{webhook_id}` | Any | Webhook detail + raw payload |
| POST | `/webhooks/{webhook_id}/replay` | Admin | Re-process a previously received webhook |
| POST | `/webhooks/shopify/orders/create` | HMAC | Shopify order webhook receiver |
| POST | `/webhooks/cratejoy/order` | HMAC | Cratejoy order webhook receiver |

---

## Customers

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/customers` | Any | Customer list with search/filter/sort |
| GET | `/customers/export-csv` | Any | Download all customers as CSV |
| GET | `/customers/{customer_id}` | Any | Customer detail — history, decisions, shipments |
| POST | `/customers/add` | Admin | Add a customer manually |
| POST | `/customers/{customer_id}/edit` | Admin | Edit customer fields |
| POST | `/customers/{customer_id}/remove` | Admin | Delete customer |
| POST | `/customers/{customer_id}/recurate` | Admin | Re-run decision engine for this customer |
| POST | `/customers/{customer_id}/override-kit` | Admin | Override assigned kit with manual selection + reason |
| POST | `/customers/{customer_id}/shipments/add` | Admin | Add historical shipment |
| POST | `/customers/{customer_id}/shipments/{shipment_id}/edit` | Admin | Edit shipment |
| POST | `/customers/{customer_id}/shipments/{shipment_id}/remove` | Admin | Remove shipment |

---

## Decisions

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/decisions` | Any | Decisions list with filter/sort |
| GET | `/decisions/export-csv` | Any | Export decisions to CSV |
| GET | `/decisions/export-veracore-csv` | Any | Export Pirate Ship CSV (approved, not yet shipped) |
| POST | `/decisions/export-sheet` | Admin | Export filtered decisions to Google Sheets |
| POST | `/decisions/bulk-action` | Admin | Bulk approve/reject/ship/recurate |
| POST | `/decisions/{decision_id}/approve` | Admin | Approve single decision → VeraCore push (background) |
| POST | `/decisions/{decision_id}/reject` | Admin | Reject a decision |
| POST | `/decisions/{decision_id}/ship` | Admin | Approve + mark as shipped |
| POST | `/decisions/{decision_id}/veracore-retry` | Admin | Retry failed VeraCore push (idempotent) |

**`/decisions/{id}/approve` query params:**
- `?via=pirateship` — approve but skip VeraCore push (manual Pirate Ship route)

---

## Kits

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/kits` | Any | Kit list with inventory levels |
| GET | `/kits/{kit_id}` | Any | Kit detail — items, history |
| POST | `/kits/add` | Admin | Add a new kit |
| POST | `/kits/{kit_id}/edit` | Admin | Edit kit (name, trimester, quantity, SKU) |
| POST | `/kits/{kit_id}/remove` | Admin | Delete kit |
| POST | `/kits/{kit_id}/items/add` | Admin | Add item to kit |
| POST | `/kits/{kit_id}/items/quick-add` | Admin | Quick-add item by name (auto-creates if missing) |
| POST | `/kits/{kit_id}/items/{item_id}/remove` | Admin | Remove item from kit |

---

## Items

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/items` | Any | Item catalog |
| POST | `/items/add` | Admin | Add item |
| POST | `/items/{item_id}/edit` | Admin | Edit item |
| POST | `/items/{item_id}/remove` | Admin | Delete item |

---

## Item Alternatives

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/item-alternatives` | Any | Substitution rules |
| POST | `/item-alternatives/add` | Admin | Add substitution rule |
| POST | `/item-alternatives/remove` | Admin | Remove substitution rule |

---

## Curation Report

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/curation-report` | Any | List all report runs |
| POST | `/curation-report/generate` | Admin | Trigger a new monthly report (async job) |
| GET | `/curation-report/job/{job_id}` | Any | Loading page while report generates |
| GET | `/curation-report/job/{job_id}/status` | Any | Job status JSON `{status, progress, run_id}` |
| GET | `/curation-report/{run_id}` | Any | View a completed report |
| POST | `/curation-report/{run_id}/delete` | Admin | Delete a report run |
| POST | `/curation-report/{run_id}/export-sheet` | Admin | Export report to Google Sheets |

---

## Forward Planner

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/forward-planner` | Any | Forward planner UI |
| POST | `/forward-planner/generate` | Admin | Run inventory projection (async job) |
| GET | `/forward-planner/job/{job_id}` | Any | Loading page |
| GET | `/forward-planner/job/{job_id}/status` | Any | Job status JSON |
| POST | `/forward-planner/{run_id}/delete` | Admin | Delete a planner run |
| POST | `/forward-planner/commit-items` | Admin | Commit inventory quantities from a plan |
| POST | `/forward-planner/clear-committed` | Admin | Clear all committed items |

---

## VeraCore

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/veracore` | Any | VeraCore ops page — connection status, pending pushes, settings |
| POST | `/veracore/sync-inventory-now` | Admin | Manual inventory sync from VeraCore → updates `quantity_available` |
| POST | `/veracore/save-settings` | Admin | Save VeraCore settings (e.g. `freight_service`) |

---

## Activity Log

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/activity` | Any | Activity log — all approve/reject/retry/error events |

---

## Settings

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/settings` | Any | Settings page — Google Sheets config, recalculation tools |
| GET | `/flow-diagram` | Any | System flow diagram |

---

## Utility / Admin APIs

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/health` | Public | Health check — returns `{"status": "ok"}` |
| POST | `/api/recalculate-all-trimesters` | Admin | Recalculate trimester for every active customer |
| POST | `/api/backfill-age-ranks` | Admin | Recompute `age_rank` for all kits |
| POST | `/api/fix-gsheet-headers` | Admin | Update Google Sheet header row to current format |
| POST | `/api/test-webhook` | Admin | Simulate a Shopify webhook with test data |
| POST | `/api/test-webhook-cratejoy` | Admin | Simulate a Cratejoy webhook with test data |
| POST | `/api/cratejoy/register-webhooks` | Admin | Register webhooks with Cratejoy API |
