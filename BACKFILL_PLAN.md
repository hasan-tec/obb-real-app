# Cratejoy Customer Backfill Plan

**Status:** Planned — not implemented
**Priority:** Medium — required to import ~20K existing Cratejoy subscribers
**Scope:** 2024, 2025, and 2026 subscriptions

---

## Problem

OBB has ~20,199 active subscribers on Cratejoy but only 5 are in the OBB database.
Webhooks have only been receiving data since March 2026, and `subscription_new` was
not registered until May 2026. All existing customers need to be imported via the
Cratejoy REST API.

---

## Key Behaviours (confirmed before implementing)

- A webhook hitting an already-imported customer does an UPDATE (not insert) — the
  customer UUID stays the same, shipment history and decisions are fully preserved.
- Only overwrites: name, address (now enriched from API), subscription status if cancelled.
- Preserves: due_date/trimester/clothing_size if DB already has them and API returns nothing.
- Joelle Dozoretz does NOT appear in any order history CSV — she is pure Cratejoy and has
  no Shopify history to cross-reference.

---

## Implementation Plan

### 1. New admin endpoint

```
POST /api/cratejoy/backfill?start_year=2024
```

- Requires admin auth (same session cookie check as all other admin routes)
- Spawns a background job (same _jobs pattern as curation runs)
- Returns { job_id } immediately; progress visible at /api/cratejoy/backfill/status/{job_id}

### 2. Pagination through Cratejoy subscriptions API

```
GET https://api.cratejoy.com/v1/subscriptions/?start_date__gte=2024-01-01&limit=50&offset=N
```

- Filter: start_date >= 2024-01-01 (covers 2024, 2025, 2026)
- Also fetch status IN (active, cancelled) -- skip purely expired pre-2024
- Log progress every 50 customers: [BACKFILL] Page N -- processed X of ~Y
- Store progress in a backfill_runs table

### 3. Per-customer upsert logic

For each subscription:

1. Check by email -- if customer exists, UPDATE only missing fields; never overwrite
   manually-entered data.
2. If new -- INSERT with platform='cratejoy', subscription_status from CJ status
3. Set cratejoy_customer_id on both new and existing records

Fields to always set (safe to overwrite):
- cratejoy_customer_id
- platform ('cratejoy' or 'both' if shopify_customer_id already exists)

Fields to set only if currently NULL in DB:
- address_line1, city, province, zip, country -- from GET /v1/customers/{id}/addresses/
- due_date, trimester, clothing_size, baby_gender, wants_daddy_item -- from GET /v1/product_survey_results/?subscription_id={id}

### 4. Rate limiting

- asyncio.sleep(0.15) between each customer
- Estimate: ~3,000 customers/hour -- 20K takes ~7 hours
- Run overnight to avoid Cratejoy rate limits

### 5. Idempotency

- Matched by email -- duplicates impossible
- Safe to re-run: existing data only enriched, never clobbered
- Decisions are NOT auto-created by the backfill -- the monthly sweep (already live)
  will create decisions for any backfilled customer on the next 1st of the month

### 6. Progress tracking

New backfill_runs table (one migration needed):

```sql
-- migrations/012_backfill_runs.sql
create table backfill_runs (
  id              uuid primary key default gen_random_uuid(),
  started_at      timestamptz default now(),
  completed_at    timestamptz,
  status          text default 'running',  -- running | completed | error
  start_year      int,
  total_fetched   int default 0,
  created_count   int default 0,
  updated_count   int default 0,
  skipped_count   int default 0,
  error_count     int default 0,
  error_message   text
);
```

---

## Files to create/modify when building this

| File | Change |
|------|--------|
| app.py | Add POST /api/cratejoy/backfill endpoint + background job function |
| migrations/012_backfill_runs.sql | Add backfill_runs table |

---

## Acceptance criteria

- [ ] All Cratejoy subscriptions from 2024-01-01 onwards are in the customers table
- [ ] Each customer has address populated (from /v1/customers/{id}/addresses/)
- [ ] Each customer has survey data populated where available (due_date, size, gender)
- [ ] Re-running the backfill does not duplicate or overwrite manually-entered data
- [ ] Progress visible via a status endpoint during the run
- [ ] On completion: created_count + updated_count + skipped_count = total_fetched
