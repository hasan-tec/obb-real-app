#!/usr/bin/env python3
"""
import_cratejoy_customers.py
============================
Imports the ACTIVE Cratejoy subscribers that are missing from the OBB database,
as CUSTOMERS ONLY — no decisions are created.

Each imported customer is:
  - enriched with survey data (due date, clothing size, gender, second-parent) and
    the subscription's shipping address, mirroring the webhook onboarding flow,
  - flagged history_pending = TRUE.

Why no decisions: these customers have already received boxes that the engine has no
record of. Creating a decision now would assign a (duplicate) welcome kit. Decisions
stay OFF until their real kit history is imported and history_pending is cleared.

Idempotent: matches existing customers by cratejoy_customer_id then email; re-running
skips anyone already present. Safe to run --dry-run first.

Prereq: migration 020 (adds customers.history_pending) must be applied first.

USAGE
  python scripts/import_cratejoy_customers.py --dry-run
  python scripts/import_cratejoy_customers.py --live
  python scripts/import_cratejoy_customers.py --dry-run --verbose
"""
import argparse
import base64
import json
import logging
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin

from dotenv import load_dotenv
from supabase import create_client

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY") or ""
CJ_ID = os.getenv("CRATEJOY_CLIENT_ID", "")
CJ_SEC = os.getenv("CRATEJOY_CLIENT_SECRET", "")

SCRIPT_DIR = Path(__file__).parent
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(SCRIPT_DIR / "import_cratejoy_customers.log", mode="w", encoding="utf-8"),
    ],
)
logger = logging.getLogger("obb_cj_import")

CJ_AUTH = base64.b64encode(f"{CJ_ID}:{CJ_SEC}".encode()).decode()
CJ_HEADERS = {"Authorization": f"Basic {CJ_AUTH}", "Accept": "application/json"}


# ---------------------------------------------------------------------------
# HTTP (Cratejoy)
# ---------------------------------------------------------------------------
def cj_get(url):
    req = urllib.request.Request(url, headers=CJ_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except Exception as ex:
        return 0, str(ex)


def cj_paginate(base):
    out = []
    url = base
    while url:
        s, d = cj_get(url)
        if not isinstance(d, dict):
            logger.warning("[CJ] pagination stopped — HTTP %s: %s", s, str(d)[:160])
            break
        out.extend(d.get("results", []))
        nx = d.get("next")
        url = urljoin(base.split("?")[0], nx) if nx else None
        time.sleep(0.1)
    return out


# ---------------------------------------------------------------------------
# Helpers (replicate app.py exactly so imported data matches webhook data)
# ---------------------------------------------------------------------------
def parse_due_date(raw):
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%d/%m/%Y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).date()
        except ValueError:
            continue
    logger.warning("[PARSE] Could not parse due_date: '%s'", raw)
    return None


def calculate_trimester(due_date, ship_date):
    # Mirror app.py calculate_trimester()
    t4 = ship_date + timedelta(days=19)
    t3 = t4 + timedelta(weeks=13)
    t2 = t3 + timedelta(weeks=14)
    if due_date <= t4:
        return 4
    elif due_date <= t3:
        return 3
    elif due_date <= t2:
        return 2
    return 1


def normalize_size(raw):
    if not raw:
        return None
    s = raw.strip().lower()
    m = {
        "s": "S", "sm": "S", "small": "S",
        "m": "M", "med": "M", "medium": "M",
        "l": "L", "lg": "L", "lrg": "L", "large": "L",
        "xl": "XL", "x-large": "XL", "xlarge": "XL", "x-lg": "XL",
        "xxl": "XL", "2xl": "XL", "xx-large": "XL", "xxlarge": "XL",
    }
    return m.get(s)


def normalize_gender(raw):
    if not raw:
        return None
    v = raw.lower().strip()
    if "boy" in v:
        return "boy"
    if "girl" in v:
        return "girl"
    return "unknown"


def normalize_daddy(raw):
    if not raw:
        return None
    v = raw.lower().strip()
    return v.startswith("yes") or v in ("y", "true", "sure", "ok", "okay")


def fetch_survey(sub_id):
    """GET /v1/product_survey_results/?subscription_id= — returns dict of extracted fields."""
    out = {"due_date": None, "clothing_size": None, "baby_gender": None, "wants_daddy_item": None}
    s, d = cj_get(f"https://api.cratejoy.com/v1/product_survey_results/?subscription_id={sub_id}")
    if not isinstance(d, dict):
        return out
    for sr in d.get("results", []):
        for ans in sr.get("answers", []):
            field = ans.get("field", {})
            fname = (field.get("name", "") if isinstance(field, dict) else "").lower().strip()
            value = str(ans.get("value", "")).strip()
            if not value:
                continue
            if "due date" in fname:
                out["due_date"] = value
            elif "clothing size" in fname:
                out["clothing_size"] = value
            elif "expecting" in fname or "whom" in fname:
                out["baby_gender"] = value
            elif "second parent" in fname or "matching apparel" in fname:
                out["wants_daddy_item"] = value
    return out


def fetch_address(sub_id, sub_obj):
    """Subscription address is authoritative. Prefer the object we already have, else fetch."""
    addr = sub_obj.get("address") if isinstance(sub_obj, dict) else None
    if not addr:
        s, d = cj_get(f"https://api.cratejoy.com/v1/subscriptions/{sub_id}/")
        if isinstance(d, dict):
            addr = d.get("address")
    if not isinstance(addr, dict):
        return {}
    return {
        "address_line1": addr.get("street") or None,
        "city": addr.get("city") or None,
        "province": addr.get("state") or None,
        "zip": addr.get("zip_code") or None,
        "country": addr.get("country") or "US",
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Import active Cratejoy subscribers missing from OBB as customers-only.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true", help="Validate + print, write NOTHING.")
    g.add_argument("--live", action="store_true", help="Insert customers (history_pending=true, no decisions).")
    ap.add_argument("--verbose", action="store_true", help="Per-customer logging.")
    args = ap.parse_args()

    logger.info("=" * 66)
    logger.info("  CRATEJOY CUSTOMER IMPORT  (%s)", "DRY RUN" if args.dry_run else "LIVE")
    logger.info("=" * 66)

    for name, val in (("SUPABASE_URL", SUPABASE_URL), ("SUPABASE key", SUPABASE_KEY),
                      ("CRATEJOY_CLIENT_ID", CJ_ID), ("CRATEJOY_CLIENT_SECRET", CJ_SEC)):
        if not val:
            logger.error("Missing %s in environment (.env) — aborting.", name)
            sys.exit(1)

    db = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Guard: confirm migration 020 applied (history_pending column exists)
    try:
        db.table("customers").select("history_pending").limit(1).execute()
    except Exception as e:
        logger.error("customers.history_pending missing — run migration 020 first. (%s)", e)
        sys.exit(1)

    # Pre-load existing customers for matching (paginate past the 1000-row default)
    existing_cids, existing_emails = set(), set()
    shopify_by_email = {}
    offset = 0
    while True:
        rows = (db.table("customers")
                .select("email, cratejoy_customer_id, shopify_customer_id")
                .range(offset, offset + 999).execute().data or [])
        for r in rows:
            if r.get("cratejoy_customer_id"):
                existing_cids.add(str(r["cratejoy_customer_id"]))
            if r.get("email"):
                existing_emails.add(r["email"].lower())
                if r.get("shopify_customer_id"):
                    shopify_by_email[r["email"].lower()] = r["shopify_customer_id"]
        if len(rows) < 1000:
            break
        offset += 1000
    logger.info("[SETUP] %d existing customer emails loaded", len(existing_emails))

    # Pull active Cratejoy subscriptions
    subs = cj_paginate("https://api.cratejoy.com/v1/subscriptions/?status=active&limit=100")
    logger.info("[CJ] %d active subscriptions", len(subs))

    # Reconcile -> missing
    missing = []
    for x in subs:
        cust = x.get("customer") or {}
        cid = str(cust.get("id", ""))
        email = (cust.get("email", "") or "").lower()
        if cid in existing_cids or (email and email in existing_emails):
            continue
        missing.append(x)
    logger.info("[RECONCILE] %d active subscribers missing from OBB", len(missing))

    created = skipped_no_email = skipped_exists = errors = 0
    today = date.today()

    for i, x in enumerate(missing):
        cust = x.get("customer") or {}
        cid = str(cust.get("id", ""))
        email = (cust.get("email", "") or "").strip().lower()
        sub_id = str(x.get("id", ""))

        if not email:
            skipped_no_email += 1
            logger.warning("[SKIP] sub %s has no customer email", sub_id)
            continue

        # Re-check existence (idempotent re-runs)
        if cid in existing_cids or email in existing_emails:
            skipped_exists += 1
            continue

        try:
            survey = fetch_survey(sub_id)
            addr = fetch_address(sub_id, x)

            due = parse_due_date(survey["due_date"]) if survey["due_date"] else None
            trimester = calculate_trimester(due, today) if due else None
            size = normalize_size(survey["clothing_size"]) if survey["clothing_size"] else None
            gender = normalize_gender(survey["baby_gender"]) if survey["baby_gender"] else None
            daddy = normalize_daddy(survey["wants_daddy_item"]) if survey["wants_daddy_item"] is not None else None

            rec = {
                "email": email,
                "first_name": cust.get("first_name") or None,
                "last_name": cust.get("last_name") or None,
                "cratejoy_customer_id": cid or None,
                "platform": "both" if email in shopify_by_email else "cratejoy",
                "subscription_status": "active",
                "history_pending": True,
            }
            rec.update({k: v for k, v in addr.items() if v})
            if due:
                rec["due_date"] = due.isoformat()
            if trimester:
                rec["trimester"] = trimester
            if size:
                rec["clothing_size"] = size
            if gender:
                rec["baby_gender"] = gender
            if daddy is not None:
                rec["wants_daddy_item"] = daddy

            if args.verbose or args.dry_run:
                logger.info("[%s] %-34s T%s size=%s sub=%s addr=%s",
                            "WOULD-ADD" if args.dry_run else "ADD",
                            email, trimester or "?", size or "?", sub_id, addr.get("city") or "?")

            if args.live:
                db.table("customers").insert(rec).execute()
                existing_cids.add(cid)
                existing_emails.add(email)
            created += 1
        except Exception as e:
            errors += 1
            logger.error("[ERROR] %s (sub %s): %s", email, sub_id, e)

        if (i + 1) % 20 == 0:
            logger.info("  ...processed %d/%d", i + 1, len(missing))

    logger.info("")
    logger.info("=" * 66)
    logger.info("  %s — %d customers, no decisions, history_pending=true",
                "WOULD IMPORT" if args.dry_run else "IMPORTED", created)
    logger.info("  skipped(no email)=%d  skipped(exists)=%d  errors=%d",
                skipped_no_email, skipped_exists, errors)
    logger.info("=" * 66)
    if args.dry_run:
        logger.info("DRY RUN — nothing written. Re-run with --live to import.")
    else:
        logger.info("DONE. Next: collect kit history from Sheena/Ting, import it, then clear history_pending.")


if __name__ == "__main__":
    main()
