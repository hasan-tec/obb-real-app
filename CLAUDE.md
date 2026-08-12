# Claude Code — OBB Workspace

> **Project:** Oh Baby Boxes (OBB) Curation Engine  
> **Client:** Ting Ting Jiang | **Owner:** Hasan Anas  
> **Stack:** Python 3.11 · FastAPI · Supabase (PostgreSQL) · Heroku  
> **Status:** Phase 1 + 2 live on Heroku. Phase 3 (VeraCore integration) active.

---

## Mandatory Behavior

1. **😊 Start EVERY response with this emoji** — no exceptions
2. **95% confidence gate** — investigate until you hit it, say "I'm 95% sure", then implement
3. **Never overcomplicate** — simplest, most traditional, best-practice solution always wins
4. **No docs/summaries at end of chat** unless explicitly asked
5. **Detailed logging everywhere** — use `logger.info/warning/error` with contextual dicts, never bare strings
6. **Surgical precision** — change only what's necessary, nothing more
7. **If blocked on API docs** (VeraCore, Shopify, Cratejoy, etc.) — ask Hasan, never guess

---

## Workflow: Think → Gather → Conquer

**Think** — analyze problem, identify functions/files, consider edge cases  
**Gather** — read ALL relevant code (full files when needed), search official docs. Keep going until 95% sure.  
**Conquer** — numbered markdown todo list → implement step by step → check off each item → continue automatically

---

## Project Structure

```
opus-obb-prototype/
├── app.py               ← Monolith: all routes, webhook handlers, decision engine
├── curation_report.py   ← Monthly curation report (APScheduler job)
├── projection_engine.py ← Inventory projection logic
├── veracore_client.py   ← VeraCore REST/SOAP client
├── veracore_sync.py     ← VeraCore sync logic
├── migrations/          ← Numbered SQL migrations (run manually in Supabase)
├── templates/           ← Jinja2 HTML templates (dashboard UI)
└── requirements.txt
```

## Dev Commands
```
cd opus-obb-prototype
pip install -r requirements.txt
uvicorn app:app --reload --port 8000    # local dev server
python -m pytest tests/                 # run tests
```

---

## Current Sprint: Phase 3 — VeraCore Fulfillment

**Goal:** Approved decisions → VeraCore order push (with CSV fallback when API unavailable)

Key files:
- `migrations/011_veracore_integration.sql` — adds `veracore_order_id`, `veracore_status`, etc. to `decisions` table
- `veracore_client.py` — VeraCore REST + SOAP client (lazy-init, creds from env)
- `app.py` — extend bulk-action handler + add sync routes

Phase 3 acceptance tests (from SOW):
- Approved rows pushed or exported as designed
- Failed pushes logged with retry-safe behavior
- No duplicate fulfillment actions on retry

**VeraCore creds:** From Ting's tenant — check env vars `VERACORE_BASE_URL`, `VERACORE_USER_ID`, `VERACORE_PASSWORD`, `VERACORE_SYSTEM_ID`  
**Source of truth for VeraCore API:** Ting's tenant Swagger UI (ask Hasan for URL) — NOT cached docs, NOT guesses

---

## Integrations

| Integration | Role | Auth |
|-------------|------|------|
| Shopify | Order webhooks, customer data | `SHOPIFY_WEBHOOK_SECRET` + `SHOPIFY_CLIENT_SECRET` |
| Cratejoy | Secondary platform webhooks | `CRATEJOY_CLIENT_ID` + `CRATEJOY_CLIENT_SECRET` |
| VeraCore | Warehouse/3PL — inventory + order push | `VERACORE_USER_ID` + `VERACORE_PASSWORD` |
| Supabase | Primary database | `SUPABASE_URL` + `SUPABASE_SERVICE_ROLE_KEY` |
| Google Sheets | Decision export fallback | `GOOGLE_SERVICE_ACCOUNT_JSON` |
| Pirate Ship | Shipping labels (CSV export) | CSV format, no API |

---

## Code Conventions

- Logging: `logger = logging.getLogger("obb")` — use it everywhere with context dicts
- Supabase: use lazy-init `get_supabase()` — never create client directly
- VeraCore: use `get_veracore_client()` — returns `None` when creds missing (callers handle that)
- Sort params must go through `ALLOWED_SORTS_*` whitelists (SQL injection prevention)
- Migration files: numbered `NNN_description.sql`, never skip numbers, run in Supabase SQL editor
- One migration per change set — plan ALL columns first, write ONE file

---

## Logging Standard

```python
logger.info("operation description", extra={"key": value, "key2": value2})
# or dict format:
logger.info("[MODULE] starting operation — key=%s", value)
logger.error("[MODULE] operation failed — error=%s", str(e), exc_info=True)
```

Always log: function entry with key inputs, success with key output, errors with full exception context.

---

## What NOT to Do

- Never touch UI templates/HTML unless the task is specifically UI — backend only
- Never guess VeraCore API endpoints or payload shapes — check Swagger or ask Hasan
- Never create duplicate migrations for the same table change
- Never remove the `ALLOWED_SORTS_*` whitelist checks
