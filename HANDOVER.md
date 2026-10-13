# OBB Curation Engine — Handover

Handover package for Oh Baby Boxes. Prepared 2026-10-06. Code state: `main` @ `e11f647`, database migrations 001–024 applied.

## Contents

| Doc | What it covers |
|---|---|
| [docs/architecture.md](docs/architecture.md) | Components, data flow, scheduler, data model, hosting |
| [docs/business-logic.md](docs/business-logic.md) | How boxes are created, how kits are chosen, every staff action, the shipping-day workflow |
| [docs/api-reference.md](docs/api-reference.md) | Every route and webhook |
| [docs/operations-runbook.md](docs/operations-runbook.md) | Deploys, migrations, local dev, tests, scripts, daily checks, troubleshooting |
| [docs/auth-setup.md](docs/auth-setup.md) | Adding and removing staff logins, roles |
| Loom walkthrough | Recorded separately (link sent with this package) |

## What OBB will own after transfer

| Asset | Today | After transfer |
|---|---|---|
| Source code | GitHub `hasan-tec/obb-real-app` | Repository transferred to OBB's GitHub account (full history, PRs) |
| Hosting | Heroku app `obb-real` (Basic dyno, ~$7/mo, no add-ons) | App transferred to OBB's Heroku account. **Same URL**, so the Shopify webhooks keep working. Config vars move with the app. |
| Database and logins | Supabase project `obb` (ref `tkcvvjxmzfjaesdhyfiy`) | Project transferred to OBB's Supabase organization. **Same URL and keys**, so no app change is needed. |
| Shopify / Cratejoy / VeraCore / Google access | OBB's own accounts | No change. The app already uses OBB's credentials. |

**What OBB needs to do:**
- Create a GitHub account (or organization), a Heroku account with a payment method, and a Supabase organization.
- Send the account names or emails.
- Accept each transfer when the email or dashboard prompt arrives.

**After the transfer, rotate these:**
- the Supabase service-role key (Supabase → Project Settings → API), then update `SUPABASE_SERVICE_ROLE_KEY` in Heroku config vars;
- the shared admin login password (Supabase → Authentication → Users).

Then remove the developer's access from all three platforms.

## Environment variables

Set in Heroku → Settings → Config Vars. They move with the app. Values are not listed here.

| Variable | Required | Source |
|---|---|---|
| `SUPABASE_URL` | yes | Supabase → Project Settings → API |
| `SUPABASE_SERVICE_ROLE_KEY` | yes | Supabase → Project Settings → API (secret, server only) |
| `SUPABASE_ANON_KEY` | fallback only | Supabase → Project Settings → API |
| `SHOPIFY_WEBHOOK_SECRET` | yes | Shopify admin → Notifications → Webhooks signing secret (or the app's client secret) |
| `SHOPIFY_CLIENT_ID`, `SHOPIFY_CLIENT_SECRET` | for tracking upload, holds, cancellations | The Shopify app's API credentials |
| `SHOPIFY_ADMIN_DOMAIN` | same | `<store>.myshopify.com` |
| `SHOPIFY_STORE_DOMAIN` | optional | Shown on the Settings page only |
| `CRATEJOY_CLIENT_ID`, `CRATEJOY_CLIENT_SECRET` | for the Cratejoy sync | Cratejoy → API settings |
| `VERACORE_BASE_URL`, `VERACORE_USER_ID`, `VERACORE_PASSWORD`, `VERACORE_SYSTEM_ID` | for VeraCore (all four, or VeraCore stays off) | From the VeraCore tenant (Brian) |
| `VERACORE_SOAP_URL`, `VERACORE_AUTH_MODE`, `VERACORE_INVENTORY_PATH`, `VERACORE_ORDER_PATH`, `VERACORE_SHIPMENT_PATH` | optional overrides | Defaults are in `app.py` |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | for Push to Sheet | Google Cloud service account key (JSON). Share the Sheet with that account's email. |
| `GOOGLE_SHEET_ID`, `GOOGLE_SHEET_NAME` | for Push to Sheet | The Sheet's URL id; tab name (default `Phase1 Decisions`) |
| `BASE_URL` | optional | Public app URL (used when registering Cratejoy webhooks) |
| `OBB_DISABLE_SCHEDULER` | never in production | Set to `1` locally, in scripts and in tests |

## Security notes for the new owner

Recommended hardening. None of this is a known incident.
- Shopify webhooks are rejected when the HMAC is wrong, but a request with **no** HMAC header is accepted. Requiring the header is a one-line change.
- `POST /webhooks/cratejoy/order` and `POST /api/cratejoy/register-webhooks` don't require a signature or login. The Cratejoy box flow now runs through the authenticated daily sync, so these can be locked down or removed.
- Login and token refresh share one Supabase client with the rest of the app. This has never been seen failing, but if staff ever report random logouts under heavy load, start there.

## Status at handover

**Live and verified:**
- the Shopify and Cratejoy intake;
- kit selection with stock reservation;
- recipient profiles;
- bulk actions in the background;
- tracking upload to the exact order or shipment;
- the cancellation and on-hold sync;
- the replay guard;
- the "Shipped on" filter for daily exports;
- the September manual-shipment history import (Shopify, and the Cratejoy T1–T4 tabs).

**Data tasks for OBB staff** (already sent to Sheena, 2026-10-01):
- Add the October kits and stock, then bulk Recurate the kitless pending boxes. Recurate the boxes on out-of-stock kits (CA-41, BC-21, BT-21).
- Add the Aynil scarf to CQ-21. The remaining CQ21 Cratejoy import can then be run (`scripts/import_manual_shipments_cj.py --skip-kits BO11`).
- Cratejoy dashboard corrections listed in `cratejoy_september_not_marked_shipped.csv` and the checklist.
- Answers on the 61-box review list (`old_pending_boxes_to_review_2026-10-02.csv`) and the remaining per-customer questions in the checklist.

**Quoted separately, not built:**
- A2 batch export ($160),
- B alerts ($340),
- ship-to override ($120),
- item filters ($80).
